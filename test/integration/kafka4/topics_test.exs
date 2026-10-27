defmodule Kayrock.Kafka4.TopicsJsonVersionsTest do
  @moduledoc """
  CreateTopics v6-v7 and DeleteTopics v5-v6 against apache/kafka:4.4.0.

  CreateTopics v7 returns the new topic's id, which DeleteTopics v6 then uses
  to delete the topic by id. Every response must decode with no leftover
  bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers
  import Kayrock.TestSupport, only: [unique_string: 0]

  @topic_already_exists 36
  @unknown_topic_or_partition 3
  @unknown_topic_id 100

  defp create_request(version, name, opts \\ []) do
    struct(Module.concat([Kayrock.CreateTopics, :"V#{version}", Request]),
      topics: [
        %{
          name: name,
          num_partitions: 2,
          replication_factor: 1,
          assignments: [],
          configs: [%{name: "cleanup.policy", value: "compact"}]
        }
      ],
      timeout_ms: 10_000,
      validate_only: Keyword.get(opts, :validate_only, false)
    )
  end

  for version <- [6, 7] do
    test "CreateTopics v#{version} creates a topic and returns its configs", %{kafka: kafka} do
      version = unquote(version)
      name = unique_string()

      assert {response, <<>>} = raw_call(kafka, create_request(version, name))
      assert [topic] = response.topics
      assert %{name: ^name, error_code: 0, num_partitions: 2, replication_factor: 1} = topic
      assert topic.topic_config_error_code == 0
      assert %{value: "compact"} = Enum.find(topic.configs, &(&1.name == "cleanup.policy"))

      if version == 7, do: assert(topic.topic_id != <<0::128>>)

      assert {again, <<>>} = raw_call(kafka, create_request(version, name))
      assert [%{error_code: @topic_already_exists}] = again.topics
    end

    test "CreateTopics v#{version} validate_only does not create the topic", %{kafka: kafka} do
      version = unquote(version)
      name = unique_string()

      assert {response, <<>>} =
               raw_call(kafka, create_request(version, name, validate_only: true))

      assert [%{name: ^name, error_code: 0}] = response.topics

      assert {real, <<>>} = raw_call(kafka, create_request(version, name))
      assert [%{error_code: 0}] = real.topics
    end
  end

  test "DeleteTopics v5 deletes by name and reports unknown topics", %{kafka: kafka} do
    name = unique_string()
    {%{topics: [%{error_code: 0}]}, <<>>} = raw_call(kafka, create_request(7, name))

    request = %Kayrock.DeleteTopics.V5.Request{
      topic_names: [name, "does-not-exist-#{name}"],
      timeout_ms: 10_000
    }

    assert {response, <<>>} = raw_call(kafka, request)
    by_name = Map.new(response.responses, &{&1.name, &1})
    assert %{error_code: 0, error_message: nil} = by_name[name]
    assert %{error_code: @unknown_topic_or_partition} = by_name["does-not-exist-#{name}"]
  end

  test "DeleteTopics v6 deletes by topic id", %{kafka: kafka} do
    name = unique_string()

    {%{topics: [%{error_code: 0, topic_id: topic_id}]}, <<>>} =
      raw_call(kafka, create_request(7, name))

    request = %Kayrock.DeleteTopics.V6.Request{
      topics: [%{name: nil, topic_id: topic_id}],
      timeout_ms: 10_000
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert [%{name: ^name, topic_id: ^topic_id, error_code: 0}] = response.responses

    assert {again, <<>>} = raw_call(kafka, request)
    assert [%{topic_id: ^topic_id, error_code: @unknown_topic_id}] = again.responses
  end

  test "DeleteTopics v6 deletes by name", %{kafka: kafka} do
    name = unique_string()
    {%{topics: [%{error_code: 0}]}, <<>>} = raw_call(kafka, create_request(6, name))

    request = %Kayrock.DeleteTopics.V6.Request{
      topics: [%{name: name, topic_id: nil}],
      timeout_ms: 10_000
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert [%{name: ^name, error_code: 0}] = response.responses
  end
end
