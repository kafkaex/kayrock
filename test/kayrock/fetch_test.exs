defmodule Kayrock.FetchTest do
  use ExUnit.Case

  alias Kayrock.Fetch.V0.Request
  alias Kayrock.Fetch.V0.Response
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message

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
              record_set: %MessageSet{
                messages: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: "foo",
                    value: "bar",
                    offset: 1
                  }
                ]
              }
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
        254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0,
        0, 2, 0, 0, 0, 17, 254, 46, 107, 157, 0, 0, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121,
        0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 17, 254>>

    expect = %Response{
      correlation_id: 1,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 56, partition: 0},
              record_set: %MessageSet{
                messages: [
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
              record_set: %MessageSet{
                messages: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: nil,
                    offset: 1,
                    value: "bar"
                  }
                ]
              }
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
              record_set: %MessageSet{
                messages: [
                  %Message{
                    attributes: 0,
                    crc: 0,
                    key: "foo",
                    offset: 1,
                    value: nil
                  }
                ]
              }
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
        0::32, 0::8, 0::8, -1::32, 3::32, "bar"::binary, 2::64, 17::32, 0::32, 0::8, 0::8, -1::32,
        3::32, "baz"::binary>>

    expect = %Response{
      correlation_id: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
              record_set: %MessageSet{
                messages: [
                  %Message{attributes: 0, crc: 0, key: nil, offset: 1, value: "bar"},
                  %Message{attributes: 0, crc: 0, key: nil, offset: 2, value: "baz"}
                ]
              }
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
              record_set: %MessageSet{
                messages: [%Message{attributes: 0, crc: 0, offset: 1, value: "bar"}]
              }
            },
            %{
              partition_header: %{error_code: 0, high_watermark: 10, partition: 1},
              record_set: %MessageSet{
                messages: [%Message{attributes: 0, crc: 0, offset: 1, value: "baz"}]
              }
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
              record_set: %MessageSet{
                messages: [%Message{attributes: 0, crc: 0, offset: 1, value: "foo"}]
              }
            }
          ]
        },
        %{
          topic: "baz",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 10, partition: 0},
              record_set: %MessageSet{
                messages: [%Message{attributes: 0, crc: 0, offset: 1, value: "bar"}]
              }
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
        38, 244, 178, 37, 0, 1, 255, 255, 255, 255, 0, 0, 0, 48, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0,
        99, 96, 128, 3, 169, 101, 15, 206, 246, 50, 48, 252, 7, 2, 32, 143, 167, 36, 181, 184, 68,
        33, 55, 181, 184, 56, 49, 61, 21, 0, 10, 31, 112, 82, 38, 0, 0, 0>>

    expect = %Response{
      correlation_id: 4,
      responses: [
        %{
          topic: "gzip_test",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 1, partition: 0},
              record_set: %MessageSet{
                messages: [
                  %Message{
                    attributes: 0,
                    crc: 2_799_750_541,
                    offset: 0,
                    value: "test message"
                  }
                ]
              }
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
      <<0, 0, 0, 3, 0, 0, 0, 1, 0, 15, 103, 122, 105, 112, 95, 98, 97, 116, 99, 104, 95, 116, 101,
        115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 180, 0, 0, 0, 0,
        0, 0, 0, 1, 0, 0, 0, 74, 112, 213, 163, 157, 0, 1, 255, 255, 255, 255, 0, 0, 0, 60, 31,
        139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3, 169, 119, 54, 19, 103, 51, 48, 252, 7, 2, 32,
        143, 39, 41, 177, 36, 57, 67, 161, 36, 181, 184, 68, 193, 16, 170, 130, 17, 164, 170, 220,
        244, 128, 34, 86, 85, 70, 0, 83, 29, 3, 53, 76, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0,
        82, 59, 149, 134, 225, 0, 1, 255, 255, 255, 255, 0, 0, 0, 68, 31, 139, 8, 0, 0, 0, 0, 0,
        0, 0, 99, 96, 0, 3, 38, 32, 150, 59, 147, 154, 199, 4, 230, 177, 100, 167, 86, 26, 2, 105,
        158, 164, 196, 146, 228, 12, 133, 146, 212, 226, 18, 5, 67, 136, 66, 6, 102, 144, 74, 182,
        111, 41, 54, 112, 149, 70, 104, 42, 141, 0, 135, 95, 114, 164, 84, 0, 0, 0>>

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
              record_set: %MessageSet{messages: [message1, message2, message3, message4]}
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
      <<0, 0, 0, 8, 0, 0, 0, 1, 0, 11, 115, 110, 97, 112, 112, 121, 95, 116, 101, 115, 116, 0, 0,
        0, 1, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 83, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0,
        0, 71, 183, 227, 95, 48, 0, 2, 255, 255, 255, 255, 0, 0, 0, 57, 130, 83, 78, 65, 80, 80,
        89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 37, 38, 0, 0, 9, 1, 120, 1, 0, 0, 0, 26, 166, 224,
        205, 141, 0, 0, 255, 255, 255, 255, 0, 0, 0, 12, 116, 101, 115, 116, 32, 109, 101, 115,
        115, 97, 103, 101>>

    expect = %Response{
      correlation_id: 8,
      responses: [
        %{
          topic: "snappy_test",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 2, partition: 1},
              record_set: %MessageSet{
                messages: [
                  %Message{attributes: 0, crc: 2_799_750_541, offset: 1, value: "test message"}
                ]
              }
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
      <<0, 0, 0, 14, 0, 0, 0, 1, 0, 17, 115, 110, 97, 112, 112, 121, 95, 98, 97, 116, 99, 104, 95,
        116, 101, 115, 116, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 105, 0,
        0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 93, 70, 199, 142, 116, 0, 2, 255, 255, 255, 255, 0, 0, 0,
        79, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 59, 84, 0, 0, 25, 1,
        16, 30, 204, 101, 110, 2, 5, 15, 76, 4, 107, 101, 121, 49, 0, 0, 0, 12, 98, 97, 116, 99,
        104, 32, 116, 101, 115, 116, 32, 1, 16, 1, 1, 32, 1, 0, 0, 0, 30, 6, 246, 100, 60, 1, 13,
        5, 42, 0, 50, 58, 42, 0, 0, 50>>

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
              record_set: %MessageSet{messages: [message1, message2]}
            }
          ]
        }
      ]
    }

    {got, <<>>} = Kayrock.Fetch.deserialize(0, response)
    assert got == expect
  end

  test "deserialize an empty record batch" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 1, 0, 20, 86, 81, 68, 78, 78, 81, 90, 67, 67, 88, 85, 84, 76, 77, 71,
        70, 68, 75, 90, 89, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

    expect = %Kayrock.Fetch.V0.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 0, partition: 0},
              record_set: nil
            }
          ],
          topic: "VQDNNQZCCXUTLMGFDKZY"
        }
      ]
    }

    {got, ""} = Kayrock.Fetch.V0.Response.deserialize(data)
    assert got == expect
  end

  test "deserialize v3 bug case" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 20, 0, 0, 2, 228, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 25, 34, 95,
        161, 130, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3,
        104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255,
        255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0,
        2, 0, 0, 0, 25, 34, 95, 161, 130, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 25, 182, 39, 239,
        145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121,
        0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 25, 34, 95, 161, 130, 1, 0, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 5, 0, 0, 0,
        25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 6, 0, 0, 0, 25, 34, 95, 161, 130, 1, 0, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0,
        0, 0, 0, 7, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 8, 0, 0, 0, 25, 34, 95, 161,
        130, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 104,
        101, 121, 0, 0, 0, 0, 0, 0, 0, 9, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255,
        255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 10,
        0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 11, 0, 0, 0, 25, 34, 95, 161, 130, 1, 0,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0,
        0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 13, 0, 0, 0, 25, 34,
        95, 161, 130, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0,
        3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 14, 0, 0, 0, 25, 34, 95, 161, 130, 1, 0, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0,
        0, 0, 0, 15, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 16, 0, 0, 0, 25, 182, 39, 239,
        145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121,
        0, 0, 0, 0, 0, 0, 0, 17, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 18, 0, 0, 0,
        25, 182, 39, 239, 145, 1, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 19, 0, 0, 0, 25, 182, 39, 239, 145, 1, 0, 255, 255,
        255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121>>

    expect = %Kayrock.Fetch.V3.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "food",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, high_watermark: 20, partition: 0},
              record_set: %Kayrock.MessageSet{
                magic: 1,
                messages: [
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 0,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 1,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 2,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 3,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 4,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 5,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 6,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 7,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 8,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 9,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 10,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 11,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 12,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 13,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 576_692_610,
                    key: nil,
                    offset: 14,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 15,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 16,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 17,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 18,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 3_056_070_545,
                    key: "",
                    offset: 19,
                    timestamp: -1,
                    value: "hey",
                    timestamp_type: 0
                  }
                ]
              }
            }
          ]
        }
      ]
    }

    {got, ""} = Kayrock.Fetch.V3.Response.deserialize(data)

    assert got == expect
  end

  test "deserialize v2 messages - single" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 2, 161,
        216, 5, 133, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 1, 52, 0, 0, 0, 0, 40, 70, 78, 71, 76, 75, 78, 76, 77, 72, 75, 74, 72, 69, 80,
        65, 69, 79, 77, 73, 83, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 1,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 0,
                  base_sequence: -1,
                  batch_length: 76,
                  batch_offset: 0,
                  crc: -1_579_678_331,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 0,
                      timestamp: -1,
                      value: "FNGLKNLMHKJHEPAEOMIS"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)

    assert got == expect
  end

  test "deserialize v2 messages - double" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 2, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0,
        255, 255, 255, 255, 0, 0, 0, 176, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 76, 0, 0, 0, 0, 2, 161,
        216, 5, 133, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 1, 52, 0, 0, 0, 0, 40, 70, 78, 71, 76, 75, 78, 76, 77, 72, 75, 74, 72, 69, 80,
        65, 69, 79, 77, 73, 83, 0, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 76, 0, 0, 0, 0, 2, 111, 58,
        148, 211, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 1, 52, 0, 0, 0, 0, 40, 77, 70, 88, 87, 79, 78, 68, 75, 69, 88, 88, 68, 66, 87,
        73, 85, 75, 68, 66, 79, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 2,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 0,
                  base_sequence: -1,
                  batch_length: 76,
                  batch_offset: 0,
                  crc: -1_579_678_331,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 0,
                      timestamp: -1,
                      value: "FNGLKNLMHKJHEPAEOMIS"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 0,
                  base_sequence: -1,
                  batch_length: 76,
                  batch_offset: 1,
                  crc: 1_866_110_163,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 1,
                      timestamp: -1,
                      value: "MFXWONDKEXXDBWIUKDBO"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)

    assert got == expect
  end

  test "deserialize v0 messages - empty record set" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 1, 0, 20, 78, 76, 68, 69, 68, 88, 68, 83, 80, 67, 68, 83, 88, 71, 87,
        72, 67, 86, 81, 77, 0, 0, 0, 1, 0, 0, 0, 0, 0, 1, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 0, 0>>

    expect = %Kayrock.Fetch.V0.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{error_code: 1, high_watermark: -1, partition: 0},
              record_set: nil
            }
          ],
          topic: "NLDEDXDSPCDSXGWHCVQM"
        }
      ]
    }

    {got, ""} = Kayrock.Fetch.V0.Response.deserialize(data)

    assert got == expect
  end

  test "deserialize v3 gzip compressed" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 83, 0, 0, 1, 14, 0, 0, 0, 0, 0, 0, 0, 80, 0, 0, 0, 78, 158, 168,
        240, 102, 1, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 56,
        31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3, 173, 181, 149, 231, 38, 50, 50, 252, 135,
        2, 168, 168, 72, 120, 96, 96, 144, 171, 179, 143, 127, 164, 115, 136, 187, 123, 164, 179,
        183, 139, 91, 132, 11, 0, 26, 55, 249, 139, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 81, 0, 0, 0,
        78, 204, 203, 91, 57, 1, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
        0, 0, 56, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3, 173, 238, 9, 255, 131, 25, 25,
        254, 67, 1, 84, 84, 196, 223, 195, 53, 210, 199, 47, 208, 49, 192, 53, 40, 48, 56, 60, 60,
        220, 203, 205, 217, 21, 0, 162, 215, 57, 253, 54, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 82, 0, 0,
        0, 78, 238, 162, 113, 219, 1, 1, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 0, 56, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 99, 96, 128, 3, 173, 70, 203, 222, 78,
        70, 134, 255, 80, 0, 21, 21, 9, 140, 12, 12, 119, 114, 14, 244, 12, 246, 112, 10, 10, 117,
        142, 8, 142, 8, 11, 119, 7, 0, 92, 38, 33, 180, 54, 0, 0, 0>>

    expect = %Kayrock.Fetch.V3.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "food",
          partition_responses: [
            %{
              partition_header: %{error_code: 0, partition: 0, high_watermark: 83},
              record_set: %Kayrock.MessageSet{
                magic: 1,
                messages: [
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    key: "",
                    timestamp: -1,
                    timestamp_type: 0,
                    crc: 2_910_441_105,
                    offset: 80,
                    value: "WQQRECLOYCTGGYCKDFXD"
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 2_341_535_571,
                    key: "",
                    offset: 81,
                    timestamp: -1,
                    timestamp_type: 0,
                    value: "OHEYLNQAPERQSWWWJFCE"
                  },
                  %Kayrock.MessageSet.Message{
                    attributes: 0,
                    compression: :none,
                    crc: 2_168_032_649,
                    key: "",
                    offset: 82,
                    timestamp: -1,
                    timestamp_type: 0,
                    value: "QYQWBCQISHBRUCXSXVWG"
                  }
                ]
              }
            }
          ]
        }
      ]
    }

    {got, ""} = Kayrock.Fetch.V3.Response.deserialize(data)
    assert got == expect
  end

  test "fetch v5 zipped" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 86, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        0, 255, 255, 255, 255, 0, 0, 1, 62, 0, 0, 0, 0, 0, 0, 0, 83, 0, 0, 0, 94, 0, 0, 0, 18, 2,
        6, 108, 40, 3, 0, 1, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0, 0, 0, 0, 51, 97, 0, 2, 141, 16, 223, 136, 64, 15,
        63, 119, 151, 208, 0, 55, 23, 151, 192, 0, 63, 47, 119, 63, 119, 6, 0, 219, 43, 70, 74,
        27, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 84, 0, 0, 0, 94, 0, 0, 0, 18, 2, 4, 196, 214, 134, 0, 1,
        0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31,
        139, 8, 0, 0, 0, 0, 0, 0, 0, 51, 97, 0, 2, 141, 240, 208, 16, 175, 176, 176, 80, 151, 72,
        31, 79, 47, 95, 55, 87, 87, 95, 143, 72, 71, 6, 0, 95, 203, 72, 110, 27, 0, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 85, 0, 0, 0, 94, 0, 0, 0, 24, 2, 97, 81, 69, 190, 0, 1, 0, 0, 0, 0, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 31, 139, 8, 0, 0, 0, 0, 0,
        0, 0, 51, 97, 0, 2, 13, 87, 159, 208, 8, 183, 16, 183, 112, 255, 8, 119, 47, 191, 32, 63,
        191, 192, 48, 31, 111, 6, 0, 210, 137, 88, 239, 27, 0, 0, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 86,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 1,
                  base_sequence: -1,
                  batch_length: 94,
                  batch_offset: 83,
                  crc: 107_751_427,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 18,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 83,
                      timestamp: -1,
                      value: "TMXQHNGDUPFDDQPNJGNG"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 1,
                  base_sequence: -1,
                  batch_length: 94,
                  batch_offset: 84,
                  crc: 80_008_838,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 18,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 84,
                      timestamp: -1,
                      value: "WUTJVVUDYLIJMFEEMHYA"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 1,
                  base_sequence: -1,
                  batch_length: 94,
                  batch_offset: 85,
                  crc: 1_632_716_222,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 24,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 85,
                      timestamp: -1,
                      value: "ELUXFTFWOXGJNRNNQVLK"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)
    assert got == expect
  end

  test "v5 fetch with snappy compression with extra data" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 206, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        0, 255, 255, 255, 255, 0, 0, 1, 74, 0, 0, 0, 0, 0, 0, 0, 203, 0, 0, 0, 98, 0, 0, 0, 33, 2,
        44, 121, 75, 132, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 0, 1, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 29, 27,
        104, 52, 0, 0, 0, 0, 40, 85, 75, 75, 70, 66, 77, 85, 90, 68, 88, 83, 77, 82, 83, 81, 65,
        87, 66, 67, 90, 0, 0, 0, 0, 0, 0, 0, 0, 204, 0, 0, 0, 98, 0, 0, 0, 33, 2, 147, 152, 102,
        235, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0,
        0, 1, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 29, 27, 104, 52, 0,
        0, 0, 0, 40, 67, 87, 85, 69, 66, 74, 83, 81, 74, 82, 68, 77, 85, 83, 73, 83, 77, 80, 82,
        66, 0, 0, 0, 0, 0, 0, 0, 0, 205, 0, 0, 0, 98, 0, 0, 0, 33, 2, 168, 136, 246, 28, 0, 2, 0,
        0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 130, 83,
        78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 29, 27, 104, 52, 0, 0, 0, 0, 40,
        77, 75, 88, 70, 81, 66, 88, 89, 86, 81, 74, 73, 67, 67, 72, 78, 76, 69, 75, 87, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 206,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 98,
                  batch_offset: 203,
                  crc: 746_146_692,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 203,
                      timestamp: -1,
                      value: "UKKFBMUZDXSMRSQAWBCZ"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 98,
                  batch_offset: 204,
                  crc: -1_818_728_725,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 204,
                      timestamp: -1,
                      value: "CWUEBJSQJRDMUSISMPRB"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 98,
                  batch_offset: 205,
                  crc: -1_467_419_108,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 205,
                      timestamp: -1,
                      value: "MKXFQBXYVQJICCHNLEKW"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)
    assert got == expect
  end

  test "v5 fetch snappy (case 2)" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 231, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        0, 255, 255, 255, 255, 0, 0, 1, 32, 0, 0, 0, 0, 0, 0, 0, 228, 0, 0, 0, 78, 0, 0, 0, 33, 2,
        106, 8, 42, 102, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 0, 1, 27, 104, 52, 0, 0, 0, 0, 40, 75, 81, 76, 84, 67, 69, 67, 88, 68, 81, 66,
        82, 81, 67, 89, 73, 65, 68, 84, 79, 0, 0, 0, 0, 0, 0, 0, 0, 229, 0, 0, 0, 76, 0, 0, 0, 33,
        2, 232, 120, 45, 102, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 1, 52, 0, 0, 0, 0, 40, 84, 87, 73, 73, 83, 71, 73, 73, 69, 72, 72, 72,
        79, 88, 70, 79, 74, 73, 86, 79, 0, 0, 0, 0, 0, 0, 0, 0, 230, 0, 0, 0, 98, 0, 0, 0, 33, 2,
        35, 151, 146, 115, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 1, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 29,
        27, 104, 52, 0, 0, 0, 0, 40, 72, 67, 85, 80, 72, 73, 85, 78, 75, 84, 67, 86, 82, 80, 66,
        86, 70, 77, 73, 82, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 231,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 78,
                  batch_offset: 228,
                  crc: 1_778_920_038,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 228,
                      timestamp: -1,
                      value: "KQLTCECXDQBRQCYIADTO"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 0,
                  base_sequence: -1,
                  batch_length: 76,
                  batch_offset: 229,
                  crc: -394_777_242,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 229,
                      timestamp: -1,
                      value: "TWIISGIIEHHHOXFOJIVO"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 98,
                  batch_offset: 230,
                  crc: 597_135_987,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 230,
                      timestamp: -1,
                      value: "HCUPHIUNKTCVRPBVFMIR"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)
    assert got == expect
  end

  test "test deserializing a record batch with an incomplete record" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 1, 0, 4, 102, 111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 231, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0, 0, 0, 0,
        0, 255, 255, 255, 255, 0, 0, 1, 34, 0, 0, 0, 0, 0, 0, 0, 228, 0, 0, 0, 78, 0, 0, 0, 33, 2,
        106, 8, 42, 102, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 0, 1, 27, 104, 52, 0, 0, 0, 0, 40, 75, 81, 76, 84, 67, 69, 67, 88, 68, 81, 66,
        82, 81, 67, 89, 73, 65, 68, 84, 79, 0, 0, 0, 0, 0, 0, 0, 0, 229, 0, 0, 0, 76, 0, 0, 0, 33,
        2, 232, 120, 45, 102, 0, 0, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 1, 52, 0, 0, 0, 0, 40, 84, 87, 73, 73, 83, 71, 73, 73, 69, 72, 72, 72,
        79, 88, 70, 79, 74, 73, 86, 79, 0, 0, 0, 0, 0, 0, 0, 0, 230, 0, 0, 0, 98, 0, 0, 0, 33, 2,
        35, 151, 146, 115, 0, 2, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 1, 130, 83, 78, 65, 80, 80, 89, 0, 0, 0, 0, 1, 0, 0, 0, 1, 0, 0, 0, 29,
        27, 104, 52, 0, 0, 0, 0, 40, 72, 67, 85, 80, 72, 73, 85, 78, 75, 84, 67, 86, 82, 80, 66,
        86, 70, 77, 73, 82, 0, 0, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 4,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 231,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 78,
                  batch_offset: 228,
                  crc: 1_778_920_038,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 228,
                      timestamp: -1,
                      value: "KQLTCECXDQBRQCYIADTO"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 0,
                  base_sequence: -1,
                  batch_length: 76,
                  batch_offset: 229,
                  crc: -394_777_242,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 229,
                      timestamp: -1,
                      value: "TWIISGIIEHHHOXFOJIVO"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 2,
                  base_sequence: -1,
                  batch_length: 98,
                  batch_offset: 230,
                  crc: 597_135_987,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: -1,
                  partition_leader_epoch: 33,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 230,
                      timestamp: -1,
                      value: "HCUPHIUNKTCVRPBVFMIR"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "food"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)
    assert got == expect
  end

  test "deserialize incomplete records batch" do
    full_data =
      <<0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 1, 0, 12, 116, 101, 115, 116, 45, 116, 111, 112, 105, 99,
        45, 50, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 30, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 202, 0, 0,
        0, 0, 2, 124, 87, 184, 205, 0, 0, 0, 0, 0, 2, 0, 0, 1, 147, 120, 5, 42, 180, 0, 0, 1, 147,
        120, 5, 42, 180, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
        0, 0, 3, 100, 0, 0, 0, 2, 49, 56, 110, 113, 120, 98, 100, 101, 99, 115, 105, 114, 104,
        103, 118, 112, 106, 116, 119, 121, 111, 117, 107, 108, 97, 122, 102, 109, 32, 49, 2, 2,
        49, 24, 113, 111, 106, 99, 100, 104, 102, 108, 122, 120, 97, 101, 100, 0, 0, 2, 2, 49, 56,
        110, 113, 120, 98, 100, 101, 99, 115, 105, 114, 104, 103, 118, 112, 106, 116, 119, 121,
        111, 117, 107, 108, 97, 122, 102, 109, 32, 50, 2, 2, 49, 24, 113, 111, 106, 99, 100, 104,
        102, 108, 122, 120, 97, 101, 100, 0, 0, 4, 2, 49, 56, 110, 113, 120, 98, 100, 101, 99,
        115, 105, 114, 104, 103, 118, 112, 106, 116, 119, 121, 111, 117, 107, 108, 97, 122, 102,
        109, 32, 51, 2, 2, 49, 24, 113, 111, 106, 99, 100, 104, 102, 108, 122, 120, 97, 101, 0, 0,
        0, 0, 0, 0, 0, 3, 0, 0, 0, 60, 0, 0, 0, 0, 2, 252, 254, 111, 122, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 147, 120, 5, 42, 180, 0, 0, 1, 147, 120, 5, 42, 180, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 20, 0, 0, 0, 2, 49, 6, 122, 97, 98, 0>>

    {response, ""} = Kayrock.Fetch.V5.Response.deserialize(full_data)

    [%{partition_responses: [partition_responses]}] = response.responses
    %{record_set: record_set} = partition_responses
    [%{records: batch_one}, %{records: batch_two}] = record_set

    # The first batch has 3 records, the second batch has 1 record
    assert List.first(batch_one).value == "nqxbdecsirhgvpjtwyouklazfm 1"
    assert List.first(batch_one).offset == 0

    assert List.last(batch_one).value == "nqxbdecsirhgvpjtwyouklazfm 3"
    assert List.last(batch_one).offset == 2

    # The second batch has 1 record
    assert List.first(batch_two).value == "zab"
    assert List.first(batch_two).offset == 3

    # Incomplete record batch, 30 => 29 in line 3
    incomplete_data =
      <<0, 0, 0, 6, 0, 0, 0, 0, 0, 0, 0, 1, 0, 12, 116, 101, 115, 116, 45, 116, 111, 112, 105, 99,
        45, 50, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 4, 0, 0, 0, 0, 0, 0, 0, 4, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 202, 0, 0,
        0, 0, 2, 124, 87, 184, 205, 0, 0, 0, 0, 0, 2, 0, 0, 1, 147, 120, 5, 42, 180, 0, 0, 1, 147,
        120, 5, 42, 180, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 0,
        0, 0, 3, 100, 0, 0, 0, 2, 49, 56, 110, 113, 120, 98, 100, 101, 99, 115, 105, 114, 104,
        103, 118, 112, 106, 116, 119, 121, 111, 117, 107, 108, 97, 122, 102, 109, 32, 49, 2, 2,
        49, 24, 113, 111, 106, 99, 100, 104, 102, 108, 122, 120, 97, 101, 100, 0, 0, 2, 2, 49, 56,
        110, 113, 120, 98, 100, 101, 99, 115, 105, 114, 104, 103, 118, 112, 106, 116, 119, 121,
        111, 117, 107, 108, 97, 122, 102, 109, 32, 50, 2, 2, 49, 24, 113, 111, 106, 99, 100, 104,
        102, 108, 122, 120, 97, 101, 100, 0, 0, 4, 2, 49, 56, 110, 113, 120, 98, 100, 101, 99,
        115, 105, 114, 104, 103, 118, 112, 106, 116, 119, 121, 111, 117, 107, 108, 97, 122, 102,
        109, 32, 51, 2, 2, 49, 24, 113, 111, 106, 99, 100, 104, 102, 108, 122, 120, 97, 101, 0, 0,
        0, 0, 0, 0, 0, 3, 0, 0, 0, 60, 0, 0, 0, 0, 2, 252, 254, 111, 122, 0, 0, 0, 0, 0, 0, 0, 0,
        1, 147, 120, 5, 42, 180, 0, 0, 1, 147, 120, 5, 42, 180, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 1, 20, 0, 0, 0, 2, 49, 6, 122, 97, 98, 0>>

    {response, _} = Kayrock.Fetch.V5.Response.deserialize(incomplete_data)

    [%{partition_responses: [partition_responses]}] = response.responses
    %{record_set: record_set} = partition_responses
    [%{records: batch_one}] = record_set

    # The first batch has 3 records, the second batch has 1 record
    assert List.first(batch_one).value == "nqxbdecsirhgvpjtwyouklazfm 1"
    assert List.first(batch_one).offset == 0

    assert List.last(batch_one).value == "nqxbdecsirhgvpjtwyouklazfm 3"
    assert List.last(batch_one).offset == 2
  end

  test "correctly handle timestamps for LogAppend" do
    data =
      <<0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 1, 0, 25, 116, 101, 115, 116, 95, 108, 111, 103, 95, 97,
        112, 112, 101, 110, 100, 95, 116, 105, 109, 101, 115, 116, 97, 109, 112, 0, 0, 0, 1, 0, 0,
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 38, 255, 255, 255, 255, 255, 255, 255, 255, 0, 0, 0, 0,
        0, 0, 0, 0, 255, 255, 255, 255, 0, 0, 1, 112, 0, 0, 0, 0, 0, 0, 0, 34, 0, 0, 0, 80, 0, 0,
        0, 0, 2, 201, 59, 211, 106, 0, 8, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255, 0,
        0, 1, 109, 29, 230, 248, 191, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 0, 1, 60, 0, 0, 0, 0, 48, 88, 78, 83, 78, 66, 83, 80, 88, 90, 71, 69, 81,
        72, 80, 81, 90, 75, 71, 69, 79, 32, 45, 32, 48, 0, 0, 0, 0, 0, 0, 0, 0, 35, 0, 0, 0, 80,
        0, 0, 0, 0, 2, 131, 101, 64, 15, 0, 8, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255, 255,
        0, 0, 1, 109, 29, 230, 248, 195, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 0, 0, 0, 1, 60, 0, 0, 0, 0, 48, 88, 78, 83, 78, 66, 83, 80, 88, 90, 71, 69,
        81, 72, 80, 81, 90, 75, 71, 69, 79, 32, 45, 32, 49, 0, 0, 0, 0, 0, 0, 0, 0, 36, 0, 0, 0,
        80, 0, 0, 0, 0, 2, 248, 80, 48, 194, 0, 8, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255, 255,
        255, 0, 0, 1, 109, 29, 230, 248, 197, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 0, 0, 0, 1, 60, 0, 0, 0, 0, 48, 88, 78, 83, 78, 66, 83, 80, 88, 90,
        71, 69, 81, 72, 80, 81, 90, 75, 71, 69, 79, 32, 45, 32, 50, 0, 0, 0, 0, 0, 0, 0, 0, 37, 0,
        0, 0, 80, 0, 0, 0, 0, 2, 45, 231, 50, 214, 0, 8, 0, 0, 0, 0, 255, 255, 255, 255, 255, 255,
        255, 255, 0, 0, 1, 109, 29, 230, 248, 199, 255, 255, 255, 255, 255, 255, 255, 255, 255,
        255, 255, 255, 255, 255, 0, 0, 0, 1, 60, 0, 0, 0, 0, 48, 88, 78, 83, 78, 66, 83, 80, 88,
        90, 71, 69, 81, 72, 80, 81, 90, 75, 71, 69, 79, 32, 45, 32, 51, 0>>

    expect = %Kayrock.Fetch.V5.Response{
      correlation_id: 8,
      responses: [
        %{
          partition_responses: [
            %{
              partition_header: %{
                aborted_transactions: [],
                error_code: 0,
                high_watermark: 38,
                last_stable_offset: -1,
                log_start_offset: 0,
                partition: 0
              },
              record_set: [
                %Kayrock.RecordBatch{
                  attributes: 8,
                  base_sequence: -1,
                  batch_length: 80,
                  batch_offset: 34,
                  crc: -918_826_134,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: 1_568_164_739_263,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 34,
                      timestamp: 1_568_164_739_263,
                      value: "XNSNBSPXZGEQHPQZKGEO - 0"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 8,
                  base_sequence: -1,
                  batch_length: 80,
                  batch_offset: 35,
                  crc: -2_090_516_465,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: 1_568_164_739_267,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 35,
                      timestamp: 1_568_164_739_267,
                      value: "XNSNBSPXZGEQHPQZKGEO - 1"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 8,
                  base_sequence: -1,
                  batch_length: 80,
                  batch_offset: 36,
                  crc: -128_962_366,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: 1_568_164_739_269,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 36,
                      timestamp: 1_568_164_739_269,
                      value: "XNSNBSPXZGEQHPQZKGEO - 2"
                    }
                  ]
                },
                %Kayrock.RecordBatch{
                  attributes: 8,
                  base_sequence: -1,
                  batch_length: 80,
                  batch_offset: 37,
                  crc: 770_126_550,
                  first_timestamp: -1,
                  last_offset_delta: 0,
                  max_timestamp: 1_568_164_739_271,
                  partition_leader_epoch: 0,
                  producer_epoch: -1,
                  producer_id: -1,
                  records: [
                    %Kayrock.RecordBatch.Record{
                      attributes: 0,
                      headers: [],
                      key: "",
                      offset: 37,
                      timestamp: 1_568_164_739_271,
                      value: "XNSNBSPXZGEQHPQZKGEO - 3"
                    }
                  ]
                }
              ]
            }
          ],
          topic: "test_log_append_timestamp"
        }
      ],
      throttle_time_ms: 0
    }

    {got, ""} = Kayrock.Fetch.V5.Response.deserialize(data)
    assert got == expect
  end
end
