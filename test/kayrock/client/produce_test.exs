defmodule Kayrock.Client.ProduceTest do
  use Kayrock.ClientCase

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  test "Simple produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])
    {:ok, _} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)

    [main_resp] = resp.responses
    [partition_resp] = main_resp.partition_responses
    %RecordBatch{records: [%Record{offset: first_offset} | _]} = partition_resp.record_set

    assert resp == %Kayrock.Fetch.V4.Response{
             correlation_id: 4,
             responses: [
               %{
                 partition_responses: [
                   %{
                     partition_header: %{
                       aborted_transactions: [],
                       error_code: 0,
                       high_watermark: offset,
                       last_stable_offset: offset,
                       partition: 0
                     },
                     record_set: %Kayrock.RecordBatch{
                       attributes: 0,
                       base_sequence: -1,
                       batch_length: 79,
                       batch_offset: first_offset,
                       crc: -784_342_914,
                       first_timestamp: -1,
                       last_offset_delta: 2,
                       max_timestamp: -1,
                       partition_leader_epoch: partition_resp.record_set.partition_leader_epoch,
                       producer_epoch: -1,
                       producer_id: -1,
                       records: [
                         %Kayrock.RecordBatch.Record{
                           attributes: 0,
                           headers: <<0>>,
                           key: nil,
                           offset: first_offset,
                           timestamp: -1,
                           value: "foo"
                         },
                         %Kayrock.RecordBatch.Record{
                           attributes: 0,
                           headers: <<0>>,
                           key: nil,
                           offset: first_offset + 1,
                           timestamp: -1,
                           value: "bar"
                         },
                         %Kayrock.RecordBatch.Record{
                           attributes: 0,
                           headers: <<0>>,
                           key: nil,
                           offset: first_offset + 2,
                           timestamp: -1,
                           value: "baz"
                         }
                       ]
                     }
                   }
                 ],
                 topic: "simple_produce"
               }
             ],
             throttle_time_ms: 0
           }
  end
end
