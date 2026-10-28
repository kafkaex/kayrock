defmodule Kayrock.Kafka4.DeleteRecordsTest do
  @moduledoc """
  DeleteRecords v2 (JSON-derived) against `apache/kafka:4.4.0`: the broker
  accepts it, the response decodes with no leftover bytes, and the log start
  offset moves.
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Wire
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  test "DeleteRecords v2 truncates the log start offset", %{kafka: kafka} do
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
              records: %RecordBatch{records: for(i <- 1..4, do: %Record{value: "#{i}"})}
            }
          ]
        }
      ]
    }

    %{responses: [%{partition_responses: [%{error_code: 0}]}]} =
      Kafka4Wire.produce(kafka, produce)

    request = %Kayrock.DeleteRecords.V2.Request{
      topics: [%{name: topic, partitions: [%{partition_index: 0, offset: 3}]}],
      timeout_ms: 5000
    }

    response = Kafka4Wire.call(kafka, request)
    assert response.throttle_time_ms == 0

    assert [%{name: ^topic, partitions: [partition], tagged_fields: []}] = response.topics
    assert partition == %{partition_index: 0, low_watermark: 3, error_code: 0, tagged_fields: []}

    # Past the high watermark: OFFSET_OUT_OF_RANGE
    request = %{
      request
      | topics: [%{name: topic, partitions: [%{partition_index: 0, offset: 99}]}]
    }

    [%{partitions: [partition]}] = Kafka4Wire.call(kafka, request).topics
    assert Kayrock.ErrorCode.code_to_atom(partition.error_code) == :offset_out_of_range
  end
end
