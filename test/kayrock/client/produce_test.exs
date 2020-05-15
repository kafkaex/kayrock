defmodule Kayrock.Client.ProduceTest do
  use Kayrock.ClientCase

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  test "Simple produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])
    {:ok, _} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)

    [main_resp] = resp.responses
    [partition_resp] = main_resp.partition_responses

    [
      %RecordBatch{
        partition_leader_epoch: partition_leader_epoch,
        records: [%Record{offset: first_offset} | _]
      }
      | _
    ] = partition_resp.record_set

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
                     record_set: [
                       %Kayrock.RecordBatch{
                         attributes: 0,
                         base_sequence: -1,
                         batch_length: 79,
                         batch_offset: first_offset,
                         crc: -784_342_914,
                         first_timestamp: -1,
                         last_offset_delta: 2,
                         max_timestamp: -1,
                         partition_leader_epoch: partition_leader_epoch,
                         producer_epoch: -1,
                         producer_id: -1,
                         records: [
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset,
                             timestamp: -1,
                             value: "foo"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 1,
                             timestamp: -1,
                             value: "bar"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 2,
                             timestamp: -1,
                             value: "baz"
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 topic: "simple_produce"
               }
             ],
             throttle_time_ms: 0
           }
  end

  test "gzip produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"], :gzip)
    {:ok, _resp} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)

    [main_resp] = resp.responses
    [partition_resp] = main_resp.partition_responses

    [
      %RecordBatch{
        partition_leader_epoch: partition_leader_epoch,
        records: [%Record{offset: first_offset} | _]
      }
      | _
    ] = partition_resp.record_set

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
                     record_set: [
                       %Kayrock.RecordBatch{
                         attributes: 1,
                         base_sequence: -1,
                         batch_length: 94,
                         batch_offset: first_offset,
                         crc: 1_821_682_799,
                         first_timestamp: -1,
                         last_offset_delta: 2,
                         max_timestamp: -1,
                         partition_leader_epoch: partition_leader_epoch,
                         producer_epoch: -1,
                         producer_id: -1,
                         records: [
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset,
                             timestamp: -1,
                             value: "foo"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 1,
                             timestamp: -1,
                             value: "bar"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 2,
                             timestamp: -1,
                             value: "baz"
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 topic: "simple_produce"
               }
             ],
             throttle_time_ms: 0
           }
  end

  test "snappy produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"], :snappy)
    {:ok, _resp} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)

    [main_resp] = resp.responses
    [partition_resp] = main_resp.partition_responses

    [
      %RecordBatch{
        partition_leader_epoch: partition_leader_epoch,
        records: [%Record{offset: first_offset} | _]
      }
      | _
    ] = partition_resp.record_set

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
                     record_set: [
                       %Kayrock.RecordBatch{
                         attributes: 2,
                         base_sequence: -1,
                         batch_length: 101,
                         batch_offset: first_offset,
                         crc: 468_182_773,
                         first_timestamp: -1,
                         last_offset_delta: 2,
                         max_timestamp: -1,
                         partition_leader_epoch: partition_leader_epoch,
                         producer_epoch: -1,
                         producer_id: -1,
                         records: [
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset,
                             timestamp: -1,
                             value: "foo"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 1,
                             timestamp: -1,
                             value: "bar"
                           },
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [],
                             key: nil,
                             offset: first_offset + 2,
                             timestamp: -1,
                             value: "baz"
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 topic: "simple_produce"
               }
             ],
             throttle_time_ms: 0
           }
  end

  test "Produce with key, value and headers works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "full_record_produce")

    headers = [
      %RecordHeader{key: "source", value: "System-X"},
      %RecordHeader{key: "type", value: "HeaderCreatedEvent"}
    ]

    record_value = "record-value-here"
    records = [%Record{headers: headers, key: "rd-k", value: record_value}]

    record_batch = %RecordBatch{
      attributes: 0,
      records: records
    }

    {:ok, _} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)

    [main_resp] = resp.responses
    [partition_resp] = main_resp.partition_responses

    [
      %RecordBatch{
        partition_leader_epoch: partition_leader_epoch,
        records: [%Record{offset: first_offset} | _]
      }
      | _
    ] = partition_resp.record_set

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
                     record_set: [
                       %Kayrock.RecordBatch{
                         attributes: 0,
                         base_sequence: -1,
                         batch_length: 118,
                         batch_offset: first_offset,
                         crc: -1_972_253_040,
                         first_timestamp: -1,
                         last_offset_delta: 0,
                         max_timestamp: -1,
                         partition_leader_epoch: partition_leader_epoch,
                         producer_epoch: -1,
                         producer_id: -1,
                         records: [
                           %Kayrock.RecordBatch.Record{
                             attributes: 0,
                             headers: [
                               %Kayrock.RecordBatch.RecordHeader{
                                 key: "source",
                                 value: "System-X"
                               },
                               %Kayrock.RecordBatch.RecordHeader{
                                 key: "type",
                                 value: "HeaderCreatedEvent"
                               }
                             ],
                             key: "rd-k",
                             offset: first_offset,
                             timestamp: -1,
                             value: "record-value-here"
                           }
                         ]
                       }
                     ]
                   }
                 ],
                 topic: "full_record_produce"
               }
             ],
             throttle_time_ms: 0
           }
  end
end
