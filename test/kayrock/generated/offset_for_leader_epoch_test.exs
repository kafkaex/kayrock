defmodule Kayrock.OffsetForLeaderEpochTest do
  @moduledoc """
  Tests for OffsetForLeaderEpoch API (V0-V3).

  API Key: 23
  Used to: Fetch offsets for leader epoch to support log truncation detection.

  Protocol structure:
  - V0-V1: Basic request with topics/partitions
  - V2: Adds replica_id and throttle_time_ms in response
  - V3: Adds current_leader_epoch
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.OffsetForLeaderEpochFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..3 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = OffsetForLeaderEpochFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 23
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = OffsetForLeaderEpochFactory.response_data(version)

        response_module = Module.concat([Kayrock.OffsetForLeaderEpoch, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.topics)
      end
    end

    test "all available versions have modules" do
      for version <- 0..3 do
        request_module = Module.concat([Kayrock, OffsetForLeaderEpoch, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, OffsetForLeaderEpoch, :"V#{version}", Response])

        assert Code.ensure_loaded?(request_module),
               "Request module #{inspect(request_module)} should exist"

        assert Code.ensure_loaded?(response_module),
               "Response module #{inspect(response_module)} should exist"
      end
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic offset for leader epoch" do
    alias Kayrock.OffsetForLeaderEpoch.V0.Request
    alias Kayrock.OffsetForLeaderEpoch.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [
          %{
            topic: "topic1",
            partitions: [%{partition: 0, leader_epoch: 5}]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 23
      assert api_version == 0
    end

    test "response with end_offset deserializes correctly" do
      {response_binary, expected} = OffsetForLeaderEpochFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.end_offset == 100
      assert partition.error_code == 0
    end
  end

  describe "V1 - adds leader_epoch in response" do
    alias Kayrock.OffsetForLeaderEpoch.V1.Response

    test "response with leader_epoch deserializes correctly" do
      {response_binary, expected} = OffsetForLeaderEpochFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.leader_epoch == 10
      assert partition.end_offset == 200
    end
  end

  describe "V2 - adds current_leader_epoch and throttle_time_ms" do
    alias Kayrock.OffsetForLeaderEpoch.V2.Request
    alias Kayrock.OffsetForLeaderEpoch.V2.Response

    test "request with current_leader_epoch serializes correctly" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        topics: [
          %{
            topic: "topic1",
            partitions: [%{partition: 0, current_leader_epoch: 12, leader_epoch: 15}]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 23
      assert api_version == 2
    end

    test "response with throttle_time_ms deserializes correctly" do
      {response_binary, expected} = OffsetForLeaderEpochFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0
    end
  end

  describe "V3 - adds current_leader_epoch" do
    alias Kayrock.OffsetForLeaderEpoch.V3.Request
    alias Kayrock.OffsetForLeaderEpoch.V3.Response

    test "request with current_leader_epoch serializes correctly" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic1",
            partitions: [%{partition: 0, current_leader_epoch: 20, leader_epoch: 18}]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 23
      assert api_version == 3
    end

    test "response deserializes correctly" do
      {response_binary, expected} = OffsetForLeaderEpochFactory.response_data(3)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.leader_epoch == 20
      assert partition.end_offset == 500
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..3 do
        module = Module.concat([Kayrock, OffsetForLeaderEpoch, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            topics: []
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..3 do
        module = Module.concat([Kayrock, OffsetForLeaderEpoch, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            topics: []
          )

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases - Truncated Binary
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..3 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = OffsetForLeaderEpochFactory.response_data(version)
        response_module = Module.concat([Kayrock.OffsetForLeaderEpoch, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  # ============================================
  # Edge Cases - Extra Bytes
  # ============================================

  describe "extra bytes handling" do
    for version <- 0..3 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = OffsetForLeaderEpochFactory.response_data(version)
        response_module = Module.concat([Kayrock.OffsetForLeaderEpoch, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..3 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.OffsetForLeaderEpoch, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 3, "UNKNOWN_TOPIC_OR_PARTITION"},
      {1, 6, "NOT_LEADER_OR_FOLLOWER"},
      {2, 4, "LEADER_NOT_AVAILABLE"},
      {3, 87, "FENCED_LEADER_EPOCH"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.OffsetForLeaderEpoch, :"V#{version}", Response])

        response_binary =
          OffsetForLeaderEpochFactory.error_response(version, error_code: error_code)

        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.topics
        [partition] = topic.partitions
        assert partition.error_code == error_code
        assert partition.end_offset == -1
      end
    end
  end
end
