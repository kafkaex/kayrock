defmodule Kayrock.MessageSerdeTest do
  use ExUnit.Case

  alias Kayrock.MessageSet
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  import Kayrock.TestSupport

  test "deserialize v0 message format" do
    msg_set_data =
      <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0,
        3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255,
        255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 17, 254, 46, 107,
        157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
        17, 254>>

    msg_set = RecordBatch.deserialize(msg_set_data)

    assert msg_set == %MessageSet{
             magic: 0,
             messages: [
               %MessageSet.Message{
                 attributes: 0,
                 crc: 4_264_455_069,
                 key: nil,
                 offset: 0,
                 value: "hey"
               },
               %MessageSet.Message{
                 attributes: 0,
                 crc: 4_264_455_069,
                 key: nil,
                 offset: 1,
                 value: "hey"
               },
               %MessageSet.Message{
                 attributes: 0,
                 crc: 4_264_455_069,
                 key: nil,
                 offset: 2,
                 value: "hey"
               }
             ]
           }
  end

  test "deserialize v2 message format" do
    msg_set_data =
      <<0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0, 79, 0, 0, 0, 0, 2, 203, 22, 157, 17, 0, 0, 0, 0, 0, 2,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 18, 0, 0, 0, 1, 6,
        102, 111, 111, 0, 18, 0, 0, 2, 1, 6, 98, 97, 114, 0, 18, 0, 0, 4, 1, 6, 98, 97, 122, 0>>

    msg_set = RecordBatch.deserialize(msg_set_data)

    assert msg_set == %RecordBatch{
             attributes: 0,
             base_sequence: -1,
             batch_length: 79,
             batch_offset: 36,
             crc: -887_710_447,
             first_timestamp: -1,
             last_offset_delta: 2,
             max_timestamp: -1,
             partition_leader_epoch: 0,
             producer_epoch: -1,
             producer_id: -1,
             records: [
               %Record{
                 attributes: 0,
                 key: nil,
                 offset: 36,
                 value: "foo"
               },
               %Record{
                 attributes: 0,
                 key: nil,
                 offset: 37,
                 value: "bar"
               },
               %Record{
                 attributes: 0,
                 key: nil,
                 offset: 38,
                 value: "baz"
               }
             ]
           }
  end

  test "serialize v2 record batch" do
    record_batch = %RecordBatch{
      attributes: 0,
      base_sequence: -1,
      batch_length: 79,
      batch_offset: 36,
      crc: -887_710_447,
      first_timestamp: -1,
      last_offset_delta: 2,
      max_timestamp: -1,
      partition_leader_epoch: 0,
      producer_epoch: -1,
      producer_id: -1,
      records: [
        %Record{
          attributes: 0,
          key: nil,
          offset: 36,
          value: "foo"
        },
        %Record{
          attributes: 0,
          key: nil,
          offset: 37,
          value: "bar"
        },
        %Record{
          attributes: 0,
          key: nil,
          offset: 38,
          value: "baz"
        }
      ]
    }

    got = IO.iodata_to_binary(RecordBatch.serialize(record_batch))

    expect =
      <<0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0, 79, 0, 0, 0, 0, 2, 203, 22, 157, 17, 0, 0, 0, 0, 0, 2,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 18, 0, 0, 0, 1, 6,
        102, 111, 111, 0, 18, 0, 0, 2, 1, 6, 98, 97, 114, 0, 18, 0, 0, 4, 1, 6, 98, 97, 122, 0>>

    assert got == expect, compare_binaries(got, expect)
  end
end
