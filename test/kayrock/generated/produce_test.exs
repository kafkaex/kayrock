defmodule Kayrock.ProduceTest do
  use ExUnit.Case

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.ProduceFactory

  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch

  # ============================================
  # V0 - Core Tests (MessageSet format)
  # ============================================

  describe "Produce V0 - request serialization" do
    test "creates a correct payload" do
      {request, expected_binary} = ProduceFactory.request_data(0)

      serialized_request = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized_request == expected_binary,
             compare_binaries(serialized_request, expected_binary)
    end

    test "correctly batches multiple request messages" do
      expected_request =
        <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102,
          111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 88, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17,
          106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121, 0, 0, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 16, 225, 27, 42, 82, 0, 0, 0, 0, 0, 0, 0, 0, 0, 2, 104, 105, 0, 0, 0, 0, 0, 0,
          0, 0, 0, 0, 0, 19, 119, 44, 195, 207, 0, 0, 0, 0, 0, 0, 0, 0, 0, 5, 104, 101, 108, 108,
          111>>

      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "foo",
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: "food",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [
                    %Message{key: "", value: "hey"},
                    %Message{key: "", value: "hi"},
                    %Message{key: "", value: "hello"}
                  ]
                }
              }
            ]
          }
        ]
      }

      serialized_request = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized_request == expected_request,
             compare_binaries(serialized_request, expected_request)
    end

    test "correctly encodes messages with gzip" do
      expected_request =
        <<0, 0, 0, 0, 0, 0, 0, 1, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 95,
          99, 108, 105, 101, 110, 116, 95, 116, 101, 115, 116, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0,
          16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 95, 116, 111, 112, 105, 99, 0, 0,
          0, 1, 0, 0, 0, 0, 0, 0, 0, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 74, 79, 44, 46, 209, 0,
          1, 255, 255, 255, 255, 0, 0, 0, 60, 31, 139, 8, 0, 0, 0, 0, 0, 0, 3, 99, 96, 128, 3,
          153, 135, 115, 4, 255, 131, 89, 172, 217, 169, 149, 10, 137, 64, 6, 103, 110, 106, 113,
          113, 98, 122, 42, 152, 3, 87, 199, 242, 37, 117, 30, 66, 93, 18, 178, 186, 36, 0, 127,
          205, 212, 97, 80, 0, 0, 0>>

      client_id = "compression_client_test"
      topic = "compressed_topic"

      messages = %MessageSet{
        messages: [
          %Message{key: "key a", value: "message a", compression: :gzip},
          %Message{key: "key b", value: "message b", compression: :gzip}
        ]
      }

      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: client_id,
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: topic,
            data: [
              %{partition: 0, record_set: messages}
            ]
          }
        ]
      }

      iolist_request = Kayrock.Request.serialize(request)

      request = :erlang.iolist_to_binary(iolist_request)

      # The exact binary contents of the message can change as zlib changes,
      # but they should remain compatible.  We test this by splitting the binary
      # up into the parts that should be the same and the parts that may differ -
      # which are the crc checkshum and the compressed part of the message.
      #
      # the byte sizes here are determined by looking at the construction of the
      # messages and headers in produce.ex
      pre_crc_header_size = 46 + byte_size(topic) + byte_size(client_id)
      crc_size = 4
      # note this includes the size of the compressed part of the binary, which
      # should be the same.
      post_crc_header_size = 10

      <<pre_crc_header::binary-size(pre_crc_header_size), _crc::binary-size(crc_size),
        post_crc_header::binary-size(post_crc_header_size),
        compressed_message_set::binary>> = request

      <<expect_pre_crc_header::binary-size(pre_crc_header_size),
        _expect_crc::binary-size(crc_size),
        expect_post_crc_header::binary-size(post_crc_header_size),
        expect_compressed_message_set::binary>> = expected_request

      assert pre_crc_header == expect_pre_crc_header
      assert post_crc_header == expect_post_crc_header

      decompressed_message_set = :zlib.gunzip(compressed_message_set)

      expect_decompressed_message_set = :zlib.gunzip(expect_compressed_message_set)

      assert decompressed_message_set == expect_decompressed_message_set
    end

    test "correctly encodes messages with snappy" do
      expected_request =
        <<0, 0, 0, 0, 0, 0, 0, 1, 0, 23, 99, 111, 109, 112, 114, 101, 115, 115, 105, 111, 110, 95,
          99, 108, 105, 101, 110, 116, 95, 116, 101, 115, 116, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0,
          16, 99, 111, 109, 112, 114, 101, 115, 115, 101, 100, 95, 116, 111, 112, 105, 99, 0, 0,
          0, 1, 0, 0, 0, 0, 0, 0, 0, 86, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 74, 67, 64, 33, 66, 0,
          2, 255, 255, 255, 255, 0, 0, 0, 60, 80, 0, 0, 25, 1, 16, 28, 225, 156, 17, 255, 5, 15,
          64, 5, 107, 101, 121, 32, 97, 0, 0, 0, 9, 109, 101, 115, 115, 97, 103, 101, 5, 13, 17,
          1, 16, 28, 4, 244, 101, 158, 5, 13, 5, 40, 52, 98, 0, 0, 0, 9, 109, 101, 115, 115, 97,
          103, 101, 32, 98>>

      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "compression_client_test",
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: "compressed_topic",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [
                    %Message{key: "key a", value: "message a", compression: :snappy},
                    %Message{key: "key b", value: "message b", compression: :snappy}
                  ]
                }
              }
            ]
          }
        ]
      }

      serialized_request = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized_request == expected_request,
             compare_binaries(serialized_request, expected_request)
    end
  end

  describe "Produce V0 - response deserialization" do
    test "correctly parses a valid response with single topic and partition" do
      {binary, expected_response} = ProduceFactory.response_data(0)

      {got, ""} = Kayrock.Produce.V0.Response.deserialize(binary)
      assert got == expected_response
    end

    test "correctly parses a valid response with multiple topics and partitions" do
      response =
        <<0::32, 2::32, 3::16, "bar"::binary, 2::32, 0::32, 0::16, 10::64, 1::32, 0::16, 20::64,
          3::16, "baz"::binary, 2::32, 0::32, 0::16, 30::64, 1::32, 0::16, 40::64>>

      expected_response = %Kayrock.Produce.V0.Response{
        correlation_id: 0,
        responses: [
          %{
            topic: "bar",
            partition_responses: [
              %{base_offset: 10, error_code: 0, partition: 0},
              %{base_offset: 20, error_code: 0, partition: 1}
            ]
          },
          %{
            topic: "baz",
            partition_responses: [
              %{base_offset: 30, error_code: 0, partition: 0},
              %{base_offset: 40, error_code: 0, partition: 1}
            ]
          }
        ]
      }

      {got, ""} = Kayrock.Produce.V0.Response.deserialize(response)
      assert got == expected_response
    end
  end

  # ============================================
  # V0 Edge Cases
  # ============================================

  describe "Produce V0 - edge cases" do
    test "empty topic_data list serializes correctly" do
      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "test",
        acks: -1,
        timeout: 1000,
        topic_data: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      expected = <<0::16, 0::16, 1::32, 4::16, "test"::binary, -1::16-signed, 1000::32, 0::32>>
      assert serialized == expected, compare_binaries(serialized, expected)
    end

    test "null key with value serializes correctly" do
      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "c",
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: "t",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [%Message{key: nil, value: "v"}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      # Verify it contains the -1 marker for null key
      assert String.contains?(serialized, <<255, 255, 255, 255>>)
    end

    test "key with null value (tombstone) serializes correctly" do
      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "c",
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: "t",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [%Message{key: "k", value: nil}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "both key and value null serializes correctly" do
      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "c",
        acks: 1,
        timeout: 10,
        topic_data: [
          %{
            topic: "t",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [%Message{key: nil, value: nil}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response with error code deserializes correctly" do
      # Response with LEADER_NOT_AVAILABLE error (5)
      response = <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 5::16, -1::64-signed>>

      {got, ""} = Kayrock.Produce.V0.Response.deserialize(response)

      assert got.correlation_id == 0
      assert length(got.responses) == 1
      [topic_response] = got.responses
      assert topic_response.topic == "bar"
      [partition_response] = topic_response.partition_responses
      assert partition_response.error_code == 5
      assert partition_response.base_offset == -1
    end

    test "response with zero offset deserializes correctly" do
      response = <<1::32, 1::32, 5::16, "topic"::binary, 1::32, 0::32, 0::16, 0::64>>

      {got, ""} = Kayrock.Produce.V0.Response.deserialize(response)

      assert got.correlation_id == 1
      [topic_resp] = got.responses
      [part_resp] = topic_resp.partition_responses
      assert part_resp.base_offset == 0
      assert part_resp.error_code == 0
    end

    test "response with max int64 offset deserializes correctly" do
      max_offset = 9_223_372_036_854_775_807
      response = <<1::32, 1::32, 5::16, "topic"::binary, 1::32, 0::32, 0::16, max_offset::64>>

      {got, ""} = Kayrock.Produce.V0.Response.deserialize(response)

      [topic_resp] = got.responses
      [part_resp] = topic_resp.partition_responses
      assert part_resp.base_offset == max_offset
    end

    test "large message (1KB) serializes correctly" do
      large_value = :crypto.strong_rand_bytes(1024)

      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "large-msg-client",
        acks: 1,
        timeout: 30_000,
        topic_data: [
          %{
            topic: "large-topic",
            data: [
              %{
                partition: 0,
                record_set: %MessageSet{
                  messages: [%Message{key: "large-key", value: large_value}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert byte_size(serialized) > 1024
    end

    test "many messages (100) in single batch serializes correctly" do
      messages =
        for i <- 1..100 do
          %Message{key: "key-#{i}", value: "value-#{i}"}
        end

      request = %Kayrock.Produce.V0.Request{
        correlation_id: 1,
        client_id: "batch-client",
        acks: 1,
        timeout: 30_000,
        topic_data: [
          %{
            topic: "batch-topic",
            data: [
              %{partition: 0, record_set: %MessageSet{messages: messages}}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  # ============================================
  # Version Compatibility (V0-V8)
  # ============================================

  describe "Produce versions compatibility" do
    test "all versions V0-V8 request serialize" do
      for version <- api_version_range(:produce) do
        {request, expected_binary} = ProduceFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request serialization failed: #{compare_binaries(serialized, expected_binary)}"
      end
    end

    test "all versions V0-V8 response deserialize" do
      for version <- api_version_range(:produce) do
        {binary, expected_struct} = ProduceFactory.response_data(version)
        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        {actual, ""} = response_module.deserialize(binary)

        assert actual == expected_struct,
               "V#{version} response deserialization failed"
      end
    end
  end

  # ============================================
  # V3+ RecordBatch Tests
  # ============================================

  describe "Produce V3+ RecordBatch features" do
    test "V3 with record headers serializes correctly" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{
                partition: 0,
                record_set: %RecordBatch{
                  records: [
                    %RecordBatch.Record{
                      key: "key",
                      value: "value",
                      headers: [
                        %RecordBatch.RecordHeader{key: "header-key", value: "header-value"},
                        %RecordBatch.RecordHeader{key: "trace-id", value: "abc123"}
                      ],
                      attributes: 0,
                      timestamp: -1,
                      offset: 0
                    }
                  ]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 0
      assert api_version == 3
    end

    test "V3 with transactional_id serializes correctly" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "transactional-client",
        transactional_id: "my-transaction-123",
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{
                partition: 0,
                record_set: %RecordBatch{
                  records: [%RecordBatch.Record{key: "k", value: "v", headers: []}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "my-transaction-123")
    end

    test "V3 with producer_id and producer_epoch serializes correctly" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{
                partition: 0,
                record_set: %RecordBatch{
                  producer_id: 12_345,
                  producer_epoch: 1,
                  base_sequence: 0,
                  records: [%RecordBatch.Record{key: "k", value: "v", headers: []}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V3 with null key in record serializes correctly" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{
                partition: 0,
                record_set: %RecordBatch{
                  records: [%RecordBatch.Record{key: nil, value: "value-only", headers: []}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V3 with null value (tombstone) serializes correctly" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{
                partition: 0,
                record_set: %RecordBatch{
                  records: [%RecordBatch.Record{key: "tombstone-key", value: nil, headers: []}]
                }
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V3 with multiple records in batch serializes correctly" do
      records =
        for i <- 0..9 do
          %RecordBatch.Record{
            key: "key-#{i}",
            value: "value-#{i}",
            headers: [],
            attributes: 0,
            timestamp: -1,
            offset: i
          }
        end

      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{partition: 0, record_set: %RecordBatch{records: records}}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V3 empty records raises InvalidRequestError" do
      request = %Kayrock.Produce.V3.Request{
        correlation_id: 3,
        client_id: "test",
        transactional_id: nil,
        acks: -1,
        timeout: 1000,
        topic_data: [
          %{
            topic: "topic",
            data: [
              %{partition: 0, record_set: %RecordBatch{records: []}}
            ]
          }
        ]
      }

      # Empty records list causes InvalidRequestError wrapping MatchError
      assert_raise Kayrock.InvalidRequestError, fn ->
        IO.iodata_to_binary(Kayrock.Request.serialize(request))
      end
    end
  end

  # ============================================
  # V2+ Response with throttle_time_ms
  # ============================================

  describe "Produce V2+ response with throttle_time_ms" do
    test "V2 response includes throttle_time_ms" do
      {binary, expected_struct} = ProduceFactory.response_data(2)

      {got, ""} = Kayrock.Produce.V2.Response.deserialize(binary)
      assert got == expected_struct
      assert got.throttle_time_ms == 500
    end

    test "V3+ responses include throttle_time_ms" do
      for version <- 3..8 do
        {binary, expected_struct} = ProduceFactory.response_data(version)
        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        {got, ""} = response_module.deserialize(binary)
        assert got == expected_struct
        assert Map.has_key?(got, :throttle_time_ms), "V#{version} missing throttle_time_ms"
      end
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "Produce V0 response edge cases" do
    alias Kayrock.Produce.V0.Response

    test "handles truncated binary at multiple points" do
      {response, _} = ProduceFactory.response_data(0)

      for truncate_at <- truncation_points(response) do
        assert_truncated_error(Response, response, truncate_at)
      end
    end

    test "handles extra trailing bytes" do
      {response, _} = ProduceFactory.response_data(0)
      extra = <<99, 88, 77>>

      assert_extra_bytes_returned(Response, response, extra)
    end

    test "handles invalid array length" do
      # Claims 100 topics but data ends
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # responses length (claims 100)
        0,
        0,
        0,
        100
      >>

      assert_raise FunctionClauseError, fn ->
        Response.deserialize(invalid)
      end
    end

    test "empty response fails with MatchError" do
      assert_raise MatchError, fn ->
        Response.deserialize(<<>>)
      end
    end
  end

  describe "Produce V2 response edge cases" do
    alias Kayrock.Produce.V2.Response

    test "handles truncated binary at multiple points" do
      {response, _} = ProduceFactory.response_data(2)

      for truncate_at <- truncation_points(response) do
        assert_truncated_error(Response, response, truncate_at)
      end
    end

    test "handles extra trailing bytes" do
      {response, _} = ProduceFactory.response_data(2)

      assert_extra_bytes_returned(Response, response, <<1, 2, 3>>)
    end

    test "empty response fails with MatchError" do
      assert_raise MatchError, fn ->
        Response.deserialize(<<>>)
      end
    end
  end

  describe "Produce V3+ response edge cases" do
    test "all V3-V8 versions handle truncated binary" do
      for version <- 3..8 do
        {response, _} = ProduceFactory.response_data(version)
        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        for truncate_at <- truncation_points(response) do
          assert_truncated_error(response_module, response, truncate_at)
        end
      end
    end

    test "all V3-V8 versions handle extra trailing bytes" do
      for version <- 3..8 do
        {response, _} = ProduceFactory.response_data(version)
        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response, <<255, 254, 253>>)
      end
    end

    test "all V3-V8 versions fail with MatchError on empty binary" do
      for version <- 3..8 do
        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  describe "Produce error responses" do
    test "V0 error response deserializes correctly" do
      error_binary = ProduceFactory.error_response(0, error_code: 5, base_offset: -1)

      {got, _rest} = Kayrock.Produce.V0.Response.deserialize(error_binary)

      [topic_response] = got.responses
      [partition_response] = topic_response.partition_responses
      assert partition_response.error_code == 5
      assert partition_response.base_offset == -1
    end

    test "V2+ error responses with throttle deserialize correctly" do
      for version <- 2..8 do
        error_binary =
          ProduceFactory.error_response(version,
            error_code: 6,
            base_offset: -1,
            throttle_time_ms: 250
          )

        response_module = Module.concat([Kayrock, Produce, :"V#{version}", Response])

        {got, _rest} = response_module.deserialize(error_binary)

        assert got.throttle_time_ms == 250
        [topic_response] = got.responses
        [partition_response] = topic_response.partition_responses
        assert partition_response.error_code == 6
        assert partition_response.base_offset == -1
      end
    end
  end
end
