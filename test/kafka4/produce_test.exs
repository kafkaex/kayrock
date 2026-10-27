defmodule Kayrock.Kafka4.ProduceTest do
  @moduledoc """
  Produce v9-v13 (JSON-derived) against `apache/kafka:4.4.0`: every new
  version is accepted, its response decodes with no leftover bytes, and the
  produced records (sent as `compact_records`) are fetched back intact. v13
  addresses the topic by ID (KIP-516).
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Wire
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    :ok = Kayrock.Client.stop(client)
    {:ok, topic: topic, topic_id: Kafka4Wire.topic_id(kafka, topic)}
  end

  defp records(prefix) do
    for i <- 1..3 do
      %Record{
        key: "#{prefix}-k#{i}",
        value: "#{prefix}-v#{i}",
        headers: [%RecordHeader{key: "n", value: "#{i}"}]
      }
    end
  end

  defp produce_request(vsn, topic, topic_id, records) do
    topic_key = if vsn >= 13, do: %{topic_id: topic_id}, else: %{name: topic}

    struct(Kayrock.Produce.get_request_struct(vsn),
      transactional_id: nil,
      acks: -1,
      timeout_ms: 5000,
      topic_data: [
        Map.put(topic_key, :partition_data, [
          %{index: 0, records: %RecordBatch{records: records}}
        ])
      ]
    )
  end

  defp fetch_all(kafka, topic_id) do
    request = %Kayrock.Fetch.V18.Request{
      max_wait_ms: 100,
      min_bytes: 0,
      max_bytes: 1_000_000,
      isolation_level: 0,
      session_id: 0,
      session_epoch: -1,
      topics: [
        %{
          topic_id: topic_id,
          partitions: [
            %{
              partition: 0,
              current_leader_epoch: -1,
              fetch_offset: 0,
              last_fetched_epoch: -1,
              log_start_offset: -1,
              partition_max_bytes: 1_000_000
            }
          ]
        }
      ],
      forgotten_topics_data: [],
      rack_id: ""
    }

    %{error_code: 0, responses: [%{topic_id: ^topic_id, partitions: [partition]}]} =
      Kafka4Wire.call(kafka, request)

    assert partition.error_code == 0
    Enum.flat_map(partition.records || [], & &1.records)
  end

  for vsn <- 9..13 do
    test "Produce v#{vsn} appends records that fetch back intact",
         %{kafka: kafka, topic: topic, topic_id: topic_id} do
      vsn = unquote(vsn)

      response = Kafka4Wire.produce(kafka, produce_request(vsn, topic, topic_id, records("a")))
      [topic_response] = response.responses

      if vsn >= 13,
        do: assert(topic_response.topic_id == topic_id),
        else: assert(topic_response.name == topic)

      [partition] = topic_response.partition_responses
      assert partition.index == 0
      assert partition.error_code == 0, inspect(partition)
      assert partition.base_offset == 0
      assert partition.record_errors == []
      assert partition.error_message == nil

      response = Kafka4Wire.produce(kafka, produce_request(vsn, topic, topic_id, records("b")))
      [%{partition_responses: [%{error_code: 0, base_offset: 3}]}] = response.responses

      if vsn >= 10, do: assert(response.node_endpoints == [])

      fetched = fetch_all(kafka, topic_id)
      assert Enum.map(fetched, & &1.offset) == Enum.to_list(0..5)

      assert Enum.map(fetched, & &1.value) ==
               ~w(a-v1 a-v2 a-v3 b-v1 b-v2 b-v3)

      assert Enum.map(fetched, & &1.key) == ~w(a-k1 a-k2 a-k3 b-k1 b-k2 b-k3)
      assert hd(fetched).headers == [%RecordHeader{key: "n", value: "1"}]
    end
  end

  test "Produce v13 to an unknown topic ID returns UNKNOWN_TOPIC_ID",
       %{kafka: kafka, topic: topic} do
    unknown = :crypto.strong_rand_bytes(16)
    response = Kafka4Wire.call(kafka, produce_request(13, topic, unknown, records("x")))

    [%{topic_id: ^unknown, partition_responses: [partition]}] = response.responses
    assert Kayrock.ErrorCode.code_to_atom(partition.error_code) == :unknown_topic_id
  end
end
