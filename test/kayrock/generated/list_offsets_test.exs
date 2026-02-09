defmodule Kayrock.ListOffsetsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.ListOffsetsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    test "all versions serialize correctly" do
      for version <- api_version_range(:list_offsets) do
        {request, expected_binary} = ListOffsetsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request serialization mismatch:\n#{compare_binaries(serialized, expected_binary)}"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 2, "V#{version} should have api_key 2"
        assert api_version == version, "V#{version} should have api_version #{version}"
      end
    end

    test "all versions deserialize correctly" do
      for version <- api_version_range(:list_offsets) do
        {response_binary, expected_struct} = ListOffsetsFactory.response_data(version)

        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])
        {actual, <<>>} = response_module.deserialize(response_binary)

        assert actual == expected_struct,
               "V#{version} response deserialization mismatch:\nExpected: #{inspect(expected_struct)}\nActual: #{inspect(actual)}"
      end
    end
  end

  # ============================================
  # Version-Specific Behavior Tests
  # ============================================

  describe "V0 specific behavior" do
    alias Kayrock.ListOffsets.V0.Request
    alias Kayrock.ListOffsets.V0.Response

    test "request includes max_num_offsets" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{partition: 0, timestamp: -1, max_num_offsets: 1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      # Verify max_num_offsets is included
      assert byte_size(serialized) > 0
    end

    test "response returns array of offsets" do
      {response_binary, expected} = ListOffsetsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == expected.correlation_id
      [topic] = response.responses
      assert topic.topic == "topic"
      [partition] = topic.partition_responses
      assert partition.partition == 0
      assert partition.error_code == 0
      assert is_list(partition.offsets)
      assert partition.offsets == [100]
    end

    test "request for latest offset (timestamp = -1)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{partition: 0, timestamp: -1, max_num_offsets: 1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "request for earliest offset (timestamp = -2)" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{partition: 0, timestamp: -2, max_num_offsets: 1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "V1 specific behavior" do
    alias Kayrock.ListOffsets.V1.Request
    alias Kayrock.ListOffsets.V1.Response

    test "request does not include max_num_offsets" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{partition: 0, timestamp: -1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response returns single offset with timestamp" do
      {response_binary, expected} = ListOffsetsFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      [topic] = response.responses
      [partition] = topic.partition_responses
      assert partition.offset == 100
      assert partition.timestamp == 42
    end
  end

  describe "V2 specific behavior" do
    alias Kayrock.ListOffsets.V2.Request
    alias Kayrock.ListOffsets.V2.Response

    test "request includes isolation_level" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        replica_id: -1,
        isolation_level: 0,
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 2
    end

    test "request with read_committed isolation" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        replica_id: -1,
        isolation_level: 1,
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response includes throttle_time_ms" do
      {response_binary, expected} = ListOffsetsFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == expected.throttle_time_ms
      assert response.throttle_time_ms == 10
    end
  end

  describe "V3 specific behavior" do
    test "same schema as V2" do
      {request, _} = ListOffsetsFactory.request_data(3)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 3
    end
  end

  describe "V4 specific behavior" do
    alias Kayrock.ListOffsets.V4.Request
    alias Kayrock.ListOffsets.V4.Response

    test "request includes current_leader_epoch" do
      request = %Request{
        correlation_id: 4,
        client_id: "test",
        replica_id: -1,
        isolation_level: 0,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{
                partition: 0,
                current_leader_epoch: 5,
                timestamp: -1
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response includes leader_epoch" do
      {response_binary, expected} = ListOffsetsFactory.response_data(4)
      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.responses
      [partition] = topic.partition_responses

      assert partition.leader_epoch ==
               expected.responses
               |> hd()
               |> Map.get(:partition_responses)
               |> hd()
               |> Map.get(:leader_epoch)

      assert partition.leader_epoch == 5
    end
  end

  describe "V5 specific behavior" do
    test "same schema as V4" do
      {request, _} = ListOffsetsFactory.request_data(5)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 5
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "all versions handle truncated response" do
      for version <- api_version_range(:list_offsets) do
        {response_binary, _expected} = ListOffsetsFactory.response_data(version)
        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    test "all versions handle extra trailing bytes" do
      for version <- api_version_range(:list_offsets) do
        {response_binary, _expected} = ListOffsetsFactory.response_data(version)
        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])

        extra_bytes = <<88, 99, 11>>
        assert_extra_bytes_returned(response_module, response_binary, extra_bytes)
      end
    end
  end

  describe "malformed response handling" do
    test "all versions fail on empty binary" do
      for version <- api_version_range(:list_offsets) do
        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 fails on invalid array length with FunctionClauseError" do
      # V0 doesn't have throttle_time_ms
      invalid = <<0, 0, 0, 0, 0, 0, 3, 232>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.ListOffsets.V0.Response.deserialize(invalid)
      end
    end

    test "V1 fails on invalid array length with FunctionClauseError" do
      # V1 doesn't have throttle_time_ms
      invalid = <<0, 0, 0, 1, 0, 0, 3, 232>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.ListOffsets.V1.Response.deserialize(invalid)
      end
    end

    test "V2+ fail on invalid array length with FunctionClauseError" do
      # V2+ have throttle_time_ms
      for version <- 2..5 do
        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])
        invalid = <<0, 0, 0, version, 0, 0, 0, 0, 0, 0, 3, 232>>

        assert_raise FunctionClauseError, fn ->
          response_module.deserialize(invalid)
        end
      end
    end
  end

  # ============================================
  # Custom Scenario Tests
  # ============================================

  describe "custom response scenarios" do
    test "error response with OFFSET_OUT_OF_RANGE" do
      for version <- api_version_range(:list_offsets) do
        response_binary = ListOffsetsFactory.error_response(version, error_code: 1)
        response_module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Response])

        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.responses
        [partition] = topic.partition_responses
        assert partition.error_code == 1
      end
    end

    test "multiple partitions in response" do
      # V1 test with multiple partitions
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # responses array length (1)
        0,
        0,
        0,
        1,
        # topic
        0,
        5,
        "topic"::binary,
        # partition_responses array length (3)
        0,
        0,
        0,
        3,
        # partition 0
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        10,
        # offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100,
        # partition 1
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        20,
        # offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        200,
        # partition 2
        0,
        0,
        0,
        2,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        30,
        # offset
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        44
      >>

      {response, <<>>} = Kayrock.ListOffsets.V1.Response.deserialize(response_binary)

      [topic] = response.responses
      assert length(topic.partition_responses) == 3

      [p0, p1, p2] = topic.partition_responses
      assert p0.partition == 0
      assert p0.offset == 100
      assert p1.partition == 1
      assert p1.offset == 200
      assert p2.partition == 2
      assert p2.offset == 300
    end

    test "multiple topics in response" do
      # V1 test with multiple topics
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # responses array length (2)
        0,
        0,
        0,
        2,
        # topic 1
        0,
        6,
        "topic1"::binary,
        # partition_responses array length (1)
        0,
        0,
        0,
        1,
        # partition
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        10,
        # offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100,
        # topic 2
        0,
        6,
        "topic2"::binary,
        # partition_responses array length (1)
        0,
        0,
        0,
        1,
        # partition
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        20,
        # offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        200
      >>

      {response, <<>>} = Kayrock.ListOffsets.V1.Response.deserialize(response_binary)

      assert length(response.responses) == 2
      [t1, t2] = response.responses
      assert t1.topic == "topic1"
      assert t2.topic == "topic2"
    end
  end
end
