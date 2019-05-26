defmodule Kayrock.FetchTest do
  defmodule V0 do
    use ExUnit.Case

    alias Kayrock.Fetch.V0.Request
    alias Kayrock.Fetch.V0.Response
    alias Kayrock.Message

    import Kayrock.TestSupport

    test "creates a valid fetch request" do
      good_request =
        <<1::16, 0::16, 1::32, 3::16, "foo"::binary, -1::32, 10::32, 1::32, 1::32, 3::16,
          "bar"::binary, 1::32, 0::32, 1::64, 10_000::32>>

      fetch_request = %Request{
        replica_id: -1,
        correlation_id: 1,
        client_id: "foo",
        max_wait_time: 10,
        min_bytes: 1,
        topics: [
          %{
            topic: "bar",
            partitions: [
              %{partition: 0, fetch_offset: 1, max_bytes: 10_000}
            ]
          }
        ]
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(fetch_request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly deserializes a valid response with a key and a value" do
      response =
        <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 32::32, 1::64, 20::32,
          0::32, 0::8, 0::8, 3::32, "foo"::binary, 3::32, "bar"::binary>>

      expected_response = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{partition: 0, error_code: 0, high_watermark: 10},
                record_set: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: "foo",
                    value: "bar",
                    offset: 1
                  }
                ]
              }
            ]
          }
        ]
      }

      {got, ""} = Response.deserialize(response)

      assert got == expected_response
    end

    test "correctly parses a fetch response with excess bytes" do
      response =
        <<0, 0, 0, 1, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 56, 0, 0, 0, 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17, 254, 46, 107, 157, 0,
          0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 17,
          254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0,
          0, 0, 2, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101,
          121, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 17, 254>>

      expect = %Response{
        correlation_id: 1,
        responses: [
          %{
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 56, partition: 0},
                record_set: [
                  %Message{
                    attributes: 0,
                    crc: 4_264_455_069,
                    key: nil,
                    offset: 0,
                    value: "hey"
                  },
                  %Message{
                    attributes: 0,
                    crc: 4_264_455_069,
                    key: nil,
                    offset: 1,
                    value: "hey"
                  },
                  %Message{
                    attributes: 0,
                    crc: 4_264_455_069,
                    key: nil,
                    offset: 2,
                    value: "hey"
                  }
                ]
              }
            ],
            topic: "food"
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)

      assert got == expect
    end

    test "correctly deserializes a valid response with a nil key and a value" do
      response =
        <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 29::32, 1::64, 17::32,
          0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary>>

      expect = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: nil,
                    offset: 1,
                    value: "bar"
                  }
                ]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with a key and a nil value" do
      response =
        <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 29::32, 1::64, 17::32,
          0::32, 0::8, 0::8, 3::32, "foo"::binary, -1::32>>

      expect = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: "foo",
                    offset: 1,
                    value: nil
                  }
                ]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "parse_response correctly parses a valid response with multiple messages" do
      response =
        <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 58::32, 1::64, 17::32,
          0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary, 2::64, 17::32, 0::32, 0::8, 0::8,
          -1::32, 3::32, "baz"::binary>>

      expect = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [
                  %Message{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"},
                  %Message{attributes: 0, crc: 0, key: nil, offset: 2, value: "baz"}
                ]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with multiple partitions" do
      response =
        <<0::32, 1::32, 3::16, "bar"::binary, 2::32, 0::32, 0::16, 10::64, 29::32, 1::64, 17::32,
          0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary, 1::32, 0::16, 10::64, 29::32, 1::64,
          17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "baz"::binary>>

      expect = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [%Message{attributes: 0, crc: 0, offset: 1, value: "bar"}]
              },
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 1},
                record_set: [%Message{attributes: 0, crc: 0, offset: 1, value: "baz"}]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with multiple topics" do
      response =
        <<0::32, 2::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64, 29::32, 1::64, 17::32,
          0::32, 0::8, 0::8, -1::32, 3::32, "foo"::binary, 3::16, "baz"::binary, 1::32, 0::32,
          0::16, 10::64, 29::32, 1::64, 17::32, 0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary>>

      expect = %Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [%Message{attributes: 0, crc: 0, offset: 1, value: "foo"}]
              }
            ]
          },
          %{
            topic: "baz",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
                record_set: [%Message{attributes: 0, crc: 0, offset: 1, value: "bar"}]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with a gzip-encoded message" do
      response =
        <<0, 0, 0, 4, 0, 0, 0, 1, 0, 9, 103, 122, 105, 112, 95, 116, 101, 115, 116, 0, 0, 0, 1, 0,
          0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 74, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 62,
          38, 244, 178, 37, 0, 1, 255, 255, 255, 255, 0, 0, 0, 48, 31, 139, 8, 0, 0, 0, 0, 0, 0,
          0, 99, 96, 128, 3, 169, 101, 15, 206, 246, 50, 48, 252, 7, 2, 32, 143, 167, 36, 181,
          184, 68, 33, 55, 181, 184, 56, 49, 61, 21, 0, 10, 31, 112, 82, 38, 0, 0, 0>>

      expect = %Response{
        correlation_id: 4,
        responses: [
          %{
            topic: "gzip_test",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 1, partition: 0},
                record_set: [
                  %Message{
                    attributes: 0,
                    crc: 2_799_750_541,
                    offset: 0,
                    value: "test message"
                  }
                ]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "parse_response correctly parses a valid response with batched gzip-encoded messages" do
      response =
        <<0, 0, 0, 3, 0, 0, 0, 1, 0, 15, 103, 122, 105, 112, 95, 98, 97, 116, 99, 104, 95, 116,
          101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 180, 0, 0,
          0, 0, 0, 0, 0, 1, 0, 0, 0, 74, 112, 213, 163, 157, 0, 1, 255, 255, 255, 255, 0, 0, 0,
          60, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3, 169, 119, 54, 19, 103, 51, 48, 252,
          7, 2, 32, 143, 39, 41, 177, 36, 57, 67, 161, 36, 181, 184, 68, 193, 16, 170, 130, 17,
          164, 170, 220, 244, 128, 34, 86, 85, 70, 0, 83, 29, 3, 53, 76, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 3, 0, 0, 0, 82, 59, 149, 134, 225, 0, 1, 255, 255, 255, 255, 0, 0, 0, 68, 31, 139,
          8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 0, 3, 38, 32, 150, 59, 147, 154, 199, 4, 230, 177, 100,
          167, 86, 26, 2, 105, 158, 164, 196, 146, 228, 12, 133, 146, 212, 226, 18, 5, 67, 136,
          66, 6, 102, 144, 74, 182, 111, 41, 54, 112, 149, 70, 104, 42, 141, 0, 135, 95, 114, 164,
          84, 0, 0, 0>>

      message1 = %Message{
        attributes: 0,
        crc: 3_996_946_843,
        key: nil,
        offset: 0,
        value: "batch test 1"
      }

      message2 = %Message{
        attributes: 0,
        crc: 2_000_011_297,
        key: nil,
        offset: 1,
        value: "batch test 2"
      }

      message3 = %Message{
        attributes: 0,
        crc: 3_429_199_362,
        key: "key1",
        offset: 2,
        value: "batch test 1"
      }

      message4 = %Message{
        attributes: 0,
        crc: 116_810_812,
        key: "key2",
        offset: 3,
        value: "batch test 2"
      }

      expect = %Response{
        correlation_id: 3,
        responses: [
          %{
            topic: "gzip_batch_test",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 4, partition: 0},
                record_set: [message1, message2, message3, message4]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with a snappy-encoded message" do
      response =
        <<0, 0, 0, 8, 0, 0, 0, 1, 0, 11, 115, 110, 97, 112, 112, 121, 95, 116, 101, 115, 116, 0,
          0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 83, 0, 0, 0, 0, 0, 0, 0, 1,
          0, 0, 0, 71, 183, 227, 95, 48, 0, 2, 255, 255, 255, 255, 0, 0, 0, 57, 130, 83, 78, 65,
          80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 37, 38, 0, 0, 9, 1, 120, 1, 0, 0, 0, 26,
          166, 224, 205, 141, 0, 0, 255, 255, 255, 255, 0, 0, 0, 12, 116, 101, 115, 116, 32, 109,
          101, 115, 115, 97, 103, 101>>

      expect = %Response{
        correlation_id: 8,
        responses: [
          %{
            topic: "snappy_test",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 2, partition: 1},
                record_set: [
                  %Message{attributes: 0, crc: 2_799_750_541, offset: 1, value: "test message"}
                ]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end

    test "correctly deserializes a valid response with batched snappy-encoded messages" do
      response =
        <<0, 0, 0, 14, 0, 0, 0, 1, 0, 17, 115, 110, 97, 112, 112, 121, 95, 98, 97, 116, 99, 104,
          95, 116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0,
          105, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 93, 70, 199, 142, 116, 0, 2, 255, 255, 255, 255,
          0, 0, 0, 79, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 59, 84, 0,
          0, 25, 1, 16, 30, 204, 101, 110, 2, 5, 15, 76, 4, 107, 101, 121, 49, 0, 0, 0, 12, 98,
          97, 116, 99, 104, 32, 116, 101, 115, 116, 32, 1, 16, 1, 1, 32, 1, 0, 0, 0, 30, 6, 246,
          100, 60, 1, 13, 5, 42, 0, 50, 58, 42, 0, 0, 50>>

      message1 = %Message{
        attributes: 0,
        crc: 3_429_199_362,
        key: "key1",
        offset: 0,
        value: "batch test 1"
      }

      message2 = %Message{
        attributes: 0,
        crc: 116_810_812,
        key: "key2",
        offset: 1,
        value: "batch test 2"
      }

      expect = %Response{
        correlation_id: 14,
        responses: [
          %{
            topic: "snappy_batch_test",
            partition_responses: [
              %{
                partition_header: %{error_code: 0, high_watermark: 2, partition: 0},
                record_set: [message1, message2]
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
      assert got == expect
    end
  end
end
