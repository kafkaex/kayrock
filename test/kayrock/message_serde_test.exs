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
      <<0, 0, 0, 91, 0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0, 79, 0, 0, 0, 0, 2, 203, 22, 157, 17, 0, 0,
        0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 18,
        0, 0, 0, 1, 6, 102, 111, 111, 0, 18, 0, 0, 2, 1, 6, 98, 97, 114, 0, 18, 0, 0, 4, 1, 6, 98,
        97, 122, 0>>

    assert got == expect, compare_binaries(got, expect)
  end

  test "message format 2 serialization with snappy compression" do
    record_batch = %Kayrock.RecordBatch{
      attributes: 2,
      base_sequence: -1,
      batch_length: nil,
      batch_offset: 0,
      crc: nil,
      first_timestamp: -1,
      last_offset_delta: -1,
      max_timestamp: -1,
      partition_leader_epoch: -1,
      producer_epoch: -1,
      producer_id: -1,
      records: [
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "foo"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "bar"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "baz"
        }
      ]
    }

    expect =
      <<0, 0, 0, 90, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 78, 255, 255, 255, 255, 2, 240, 195, 168,
        31, 0, 2, 0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0,
        0, 3, 30, 36, 18, 0, 0, 0, 1, 6, 102, 111, 111, 0, 9, 10, 52, 98, 97, 114, 0, 18, 0, 0, 0,
        1, 6, 98, 97, 122, 0>>

    got = IO.iodata_to_binary(RecordBatch.serialize(record_batch))
    assert got == expect, compare_binaries(got, expect)
  end

  test "message format 2 deserialization with snappy compression" do
    data =
      <<0, 0, 0, 0, 0, 0, 0, 126, 0, 0, 0, 101, 0, 0, 0, 4, 2, 27, 231, 230, 245, 0, 2, 0, 0, 0,
        2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 130, 83, 78,
        65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 32, 30, 116, 18, 0, 0, 0, 1, 6, 102,
        111, 111, 0, 18, 0, 0, 2, 1, 6, 98, 97, 114, 0, 18, 0, 0, 4, 1, 6, 98, 97, 122, 0>>

    expect = %Kayrock.RecordBatch{
      attributes: 2,
      base_sequence: -1,
      batch_length: 101,
      batch_offset: 126,
      crc: 468_182_773,
      first_timestamp: -1,
      last_offset_delta: 2,
      max_timestamp: -1,
      partition_leader_epoch: 4,
      producer_epoch: -1,
      producer_id: -1,
      records: [
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 126,
          timestamp: -1,
          value: "foo"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 127,
          timestamp: -1,
          value: "bar"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 128,
          timestamp: -1,
          value: "baz"
        }
      ]
    }

    got = RecordBatch.deserialize(data)
    assert got == expect
  end

  test "serialize v2 message with gzip compression" do
    record_batch = %Kayrock.RecordBatch{
      attributes: 1,
      base_sequence: -1,
      batch_length: nil,
      batch_offset: 0,
      crc: nil,
      first_timestamp: -1,
      last_offset_delta: -1,
      max_timestamp: -1,
      partition_leader_epoch: -1,
      producer_epoch: -1,
      producer_id: -1,
      records: [
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "foo"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "bar"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 0,
          timestamp: -1,
          value: "baz"
        }
      ]
    }

    expect =
      <<0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 88, 255, 255, 255, 255, 2, 114, 69, 255,
        84, 0, 1, 0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0,
        0, 3, 31, 139, 8, 0, 0, 0, 0, 0, 0, 19, 19, 98, 96, 96, 96, 100, 75, 203, 207, 103, 16, 2,
        179, 146, 18, 139, 224, 172, 42, 6, 0, 160, 136, 44, 185, 30, 0, 0, 0>>

    got = IO.iodata_to_binary(RecordBatch.serialize(record_batch))

    assert got == expect, compare_binaries(got, expect)
  end

  test "deserialize v2 message with gzip compression" do
    data =
      <<0, 0, 0, 0, 0, 0, 0, 132, 0, 0, 0, 94, 0, 0, 0, 4, 2, 108, 148, 172, 111, 0, 1, 0, 0, 0,
        2, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 31, 139, 8,
        0, 0, 0, 0, 0, 0, 0, 19, 98, 96, 96, 96, 100, 75, 203, 207, 103, 16, 98, 96, 96, 98, 100,
        75, 74, 44, 2, 177, 88, 64, 172, 42, 6, 0, 116, 60, 95, 153, 30, 0, 0, 0>>

    expect = %Kayrock.RecordBatch{
      attributes: 1,
      base_sequence: -1,
      batch_length: 94,
      batch_offset: 132,
      crc: 1_821_682_799,
      first_timestamp: -1,
      last_offset_delta: 2,
      max_timestamp: -1,
      partition_leader_epoch: 4,
      producer_epoch: -1,
      producer_id: -1,
      records: [
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 132,
          timestamp: -1,
          value: "foo"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 133,
          timestamp: -1,
          value: "bar"
        },
        %Kayrock.RecordBatch.Record{
          attributes: 0,
          headers: <<0>>,
          key: nil,
          offset: 134,
          timestamp: -1,
          value: "baz"
        }
      ]
    }

    got = RecordBatch.deserialize(data)
    assert got == expect
  end
end
