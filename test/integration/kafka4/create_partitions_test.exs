defmodule Kayrock.Kafka4.CreatePartitionsJsonVersionsTest do
  @moduledoc """
  CreatePartitions v2-v3 against apache/kafka:4.4.0: partitions are added
  and every response decodes with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @invalid_partitions 37

  defp request(version, topic, count, opts \\ []) do
    struct(Module.concat([Kayrock.CreatePartitions, :"V#{version}", Request]),
      topics: [%{name: topic, count: count, assignments: Keyword.get(opts, :assignments)}],
      timeout_ms: 10_000,
      validate_only: Keyword.get(opts, :validate_only, false)
    )
  end

  for version <- [2, 3] do
    test "v#{version} adds partitions", %{kafka: kafka} do
      version = unquote(version)
      {:ok, client} = build_client(kafka)
      topic = create_kafka4_topic(client, partitions: 1)

      assert {response, <<>>} = raw_call(kafka, request(version, topic, 2, validate_only: true))
      assert [%{name: ^topic, error_code: 0, error_message: nil}] = response.results

      assert {response, <<>>} =
               raw_call(
                 kafka,
                 request(version, topic, 3,
                   assignments: [[1], [1]] |> Enum.map(&%{broker_ids: &1})
                 )
               )

      assert [%{name: ^topic, error_code: 0}] = response.results
      assert :ok = wait_for_leaders(client, topic, 3)

      assert {response, <<>>} = raw_call(kafka, request(version, topic, 2))

      assert [%{name: ^topic, error_code: @invalid_partitions, error_message: message}] =
               response.results

      assert is_binary(message)
    end
  end
end
