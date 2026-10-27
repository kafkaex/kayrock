defmodule Kayrock.Kafka4.LogDirsJsonVersionsTest do
  @moduledoc """
  DescribeLogDirs v2-v5 and AlterReplicaLogDirs v2 against
  apache/kafka:4.4.0 (one log dir). Every response must decode with no
  leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @log_dir_not_found 57

  defp describe(kafka, version, topics) do
    request =
      struct(Module.concat([Kayrock.DescribeLogDirs, :"V#{version}", Request]), topics: topics)

    raw_call(kafka, request)
  end

  # The log dir report lags topic creation slightly; poll until every
  # partition of `topic` is listed.
  defp describe_until_listed(kafka, version, topic, partitions, retries \\ 50) do
    {response, rest} = describe(kafka, version, [%{topic: topic, partitions: partitions}])

    listed =
      for %{topics: topics} <- response.results,
          %{name: ^topic, partitions: parts} <- topics,
          part <- parts,
          do: part.partition_index

    if Enum.sort(listed) == partitions or retries == 0 do
      {response, rest}
    else
      Process.sleep(100)
      describe_until_listed(kafka, version, topic, partitions, retries - 1)
    end
  end

  for version <- 2..5 do
    test "DescribeLogDirs v#{version} describes a topic's partitions", %{kafka: kafka} do
      version = unquote(version)
      {:ok, client} = build_client(kafka)
      topic = create_kafka4_topic(client, partitions: 2)

      assert {response, <<>>} = describe_until_listed(kafka, version, topic, [0, 1])
      if version >= 3, do: assert(response.error_code == 0)

      assert [result] = response.results
      assert result.error_code == 0
      assert is_binary(result.log_dir)
      assert [%{name: ^topic, partitions: partitions}] = result.topics
      assert partitions |> Enum.map(& &1.partition_index) |> Enum.sort() == [0, 1]

      if version >= 4 do
        assert result.total_bytes > 0
        assert result.usable_bytes > 0
      end

      if version >= 5, do: assert(result.is_cordoned == 0)

      # null topics: every topic
      assert {all, <<>>} = describe(kafka, version, nil)
      assert [%{topics: all_topics}] = all.results
      assert Enum.any?(all_topics, &(&1.name == topic))
    end
  end

  test "AlterReplicaLogDirs v2", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    {%{results: [%{log_dir: log_dir}]}, <<>>} = describe(kafka, 5, nil)

    same_dir = %Kayrock.AlterReplicaLogDirs.V2.Request{
      dirs: [%{path: log_dir, topics: [%{name: topic, partitions: [0]}]}]
    }

    assert {response, <<>>} = raw_call(kafka, same_dir)

    assert [%{topic_name: ^topic, partitions: [%{partition_index: 0, error_code: 0}]}] =
             response.results

    unknown_dir = %Kayrock.AlterReplicaLogDirs.V2.Request{
      dirs: [%{path: "/no/such/dir", topics: [%{name: topic, partitions: [0]}]}]
    }

    assert {response, <<>>} = raw_call(kafka, unknown_dir)

    assert [%{topic_name: ^topic, partitions: [%{error_code: @log_dir_not_found}]}] =
             response.results
  end
end
