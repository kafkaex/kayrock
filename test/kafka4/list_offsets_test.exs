defmodule Kayrock.Kafka4.ListOffsetsTest do
  @moduledoc """
  ListOffsets v6-v11 (JSON-derived) against `apache/kafka:4.4.0`: every new
  version is accepted and decodes with no leftover bytes; earliest (-2),
  latest (-1), by-timestamp and, from v7, max-timestamp (-3) lookups return
  the right offsets.
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Wire
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  @base_ts 1_700_000_000_000

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    :ok = Kayrock.Client.stop(client)

    # Offsets 0..3 with timestamps base, base+10, base+30 (max), base+20
    records =
      for {offset_ts, i} <- Enum.with_index([0, 10, 30, 20]) do
        %Record{value: "v#{i}", timestamp: @base_ts + offset_ts}
      end

    produce = %Kayrock.Produce.V12.Request{
      transactional_id: nil,
      acks: -1,
      timeout_ms: 5000,
      topic_data: [
        %{name: topic, partition_data: [%{index: 0, records: %RecordBatch{records: records}}]}
      ]
    }

    %{responses: [%{partition_responses: [%{error_code: 0}]}]} =
      Kafka4Wire.produce(kafka, produce)

    {:ok, topic: topic}
  end

  defp list_offset(kafka, vsn, topic, timestamp) do
    request =
      struct(Kayrock.ListOffsets.get_request_struct(vsn),
        replica_id: -1,
        isolation_level: 0,
        topics: [
          %{
            name: topic,
            partitions: [%{partition_index: 0, current_leader_epoch: -1, timestamp: timestamp}]
          }
        ],
        timeout_ms: 5000
      )

    response = Kafka4Wire.call(kafka, request)
    assert response.throttle_time_ms == 0
    [%{name: ^topic, partitions: [partition]}] = response.topics
    assert partition.partition_index == 0
    assert partition.error_code == 0, inspect(partition)
    partition
  end

  for vsn <- 6..11 do
    test "ListOffsets v#{vsn} earliest / latest / by timestamp", %{kafka: kafka, topic: topic} do
      vsn = unquote(vsn)

      assert %{offset: 0} = list_offset(kafka, vsn, topic, -2)
      latest = list_offset(kafka, vsn, topic, -1)
      assert latest.offset == 4
      assert latest.leader_epoch >= 0

      # first offset whose timestamp is >= base + 15 is offset 2 (base + 30)
      assert %{offset: 2, timestamp: ts} = list_offset(kafka, vsn, topic, @base_ts + 15)
      assert ts == @base_ts + 30
    end
  end

  for vsn <- 7..11 do
    test "ListOffsets v#{vsn} max timestamp (-3)", %{kafka: kafka, topic: topic} do
      assert %{offset: 2, timestamp: ts} = list_offset(kafka, unquote(vsn), topic, -3)
      assert ts == @base_ts + 30
    end
  end
end
