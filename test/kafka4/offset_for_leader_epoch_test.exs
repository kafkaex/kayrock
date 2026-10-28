defmodule Kayrock.Kafka4.OffsetForLeaderEpochTest do
  @moduledoc """
  OffsetForLeaderEpoch v4 (JSON-derived) against `apache/kafka:4.4.0`: the
  broker accepts it, the response decodes with no leftover bytes, and the end
  offset of the current leader epoch is the log end offset.
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Wire
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  test "OffsetForLeaderEpoch v4 returns the end offset of the leader epoch", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    :ok = Kayrock.Client.stop(client)

    produce = %Kayrock.Produce.V12.Request{
      transactional_id: nil,
      acks: -1,
      timeout_ms: 5000,
      topic_data: [
        %{
          name: topic,
          partition_data: [
            %{
              index: 0,
              records: %RecordBatch{records: [%Record{value: "a"}, %Record{value: "b"}]}
            }
          ]
        }
      ]
    }

    %{responses: [%{partition_responses: [%{error_code: 0}]}]} =
      Kafka4Wire.produce(kafka, produce)

    # The leader epoch of a freshly created single-replica partition, from
    # ListOffsets (latest)
    %{topics: [%{partitions: [%{error_code: 0, leader_epoch: epoch}]}]} =
      Kafka4Wire.call(kafka, %Kayrock.ListOffsets.V11.Request{
        replica_id: -1,
        isolation_level: 0,
        topics: [
          %{
            name: topic,
            partitions: [%{partition_index: 0, current_leader_epoch: -1, timestamp: -1}]
          }
        ],
        timeout_ms: 5000
      })

    request = %Kayrock.OffsetForLeaderEpoch.V4.Request{
      replica_id: -1,
      topics: [
        %{
          topic: topic,
          partitions: [%{partition: 0, current_leader_epoch: epoch, leader_epoch: epoch}]
        }
      ]
    }

    response = Kafka4Wire.call(kafka, request)
    assert response.throttle_time_ms == 0
    assert response.tagged_fields == []

    assert [%{topic: ^topic, partitions: [partition], tagged_fields: []}] = response.topics

    assert partition == %{
             error_code: 0,
             partition: 0,
             leader_epoch: epoch,
             end_offset: 2,
             tagged_fields: []
           }

    # A stale current_leader_epoch is fenced
    stale = %{
      request
      | topics: [
          %{
            topic: topic,
            partitions: [%{partition: 0, current_leader_epoch: epoch + 5, leader_epoch: epoch}]
          }
        ]
    }

    [%{partitions: [partition]}] = Kafka4Wire.call(kafka, stale).topics
    assert Kayrock.ErrorCode.code_to_atom(partition.error_code) == :unknown_leader_epoch
  end
end
