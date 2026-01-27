defmodule Kayrock.HeartbeatTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.HeartbeatFactory

  # ============================================
  # Primary Tests - Version Compatibility Loop
  # ============================================

  describe "versions compatibility" do
    for version <- 0..4 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = HeartbeatFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = HeartbeatFactory.response_data(version)

        {actual_struct, <<>>} =
          Module.concat([Kayrock.Heartbeat, :"V#{version}", Response]).deserialize(
            response_binary
          )

        assert actual_struct == expected_struct
      end
    end
  end

  # ============================================
  # Version-Specific Feature Tests
  # ============================================

  describe "V3+ group_instance_id support" do
    test "V3 serializes request with nil group_instance_id" do
      request = %Kayrock.Heartbeat.V3.Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        group_instance_id: nil
      }

      expected = <<
        # api_key (12)
        0,
        12,
        # api_version (3)
        0,
        3,
        # correlation_id
        0,
        0,
        0,
        3,
        # client_id
        0,
        4,
        "test"::binary,
        # group_id
        0,
        5,
        "group"::binary,
        # generation_id
        0,
        0,
        0,
        1,
        # member_id
        0,
        6,
        "member"::binary,
        # group_instance_id (nullable string, null)
        255,
        255
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    test "V4 serializes request with non-nil group_instance_id" do
      request = %Kayrock.Heartbeat.V4.Request{
        correlation_id: 4,
        client_id: "test",
        group_id: "group",
        generation_id: 2,
        member_id: "member",
        group_instance_id: "instance-1",
        tagged_fields: []
      }

      expected = <<
        # api_key (12)
        0,
        12,
        # api_version (4)
        0,
        4,
        # correlation_id
        0,
        0,
        0,
        4,
        # client_id
        0,
        4,
        "test"::binary,
        # flexible header marker (tagged fields)
        0,
        # group_id (compact string: length+1 = 6)
        6,
        "group"::binary,
        # generation_id
        0,
        0,
        0,
        2,
        # member_id (compact string: length+1 = 7)
        7,
        "member"::binary,
        # group_instance_id (compact nullable string: length+1 = 11)
        11,
        "instance-1"::binary,
        # tagged_fields
        0
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end
  end

  describe "V0 error response deserialization" do
    test "deserializes REBALANCE_IN_PROGRESS error" do
      response_binary = HeartbeatFactory.error_response(0, error_code: 27, correlation_id: 1)
      {response, <<>>} = Kayrock.Heartbeat.V0.Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.error_code == 27
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..4 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = HeartbeatFactory.response_data(version)

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(
            Module.concat([Kayrock.Heartbeat, :"V#{version}", Response]),
            response_binary,
            truncate_at
          )
        end
      end
    end
  end

  describe "extra bytes handling" do
    for version <- 0..4 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = HeartbeatFactory.response_data(version)

        assert_extra_bytes_returned(
          Module.concat([Kayrock.Heartbeat, :"V#{version}", Response]),
          response_binary,
          <<1, 2, 3>>
        )
      end
    end
  end

  describe "malformed response handling" do
    for version <- 0..4 do
      test "V#{version} empty binary fails" do
        version = unquote(version)

        assert_raise MatchError, fn ->
          Module.concat([Kayrock.Heartbeat, :"V#{version}", Response]).deserialize(<<>>)
        end
      end
    end

    test "V0 partial response fails" do
      assert_raise FunctionClauseError, fn ->
        Kayrock.Heartbeat.V0.Response.deserialize(<<0, 0, 0, 0>>)
      end
    end

    for version <- 1..4 do
      test "V#{version} partial response fails" do
        version = unquote(version)

        assert_raise FunctionClauseError, fn ->
          Module.concat([Kayrock.Heartbeat, :"V#{version}", Response]).deserialize(<<0, 0, 0, 0>>)
        end
      end
    end
  end
end
