defmodule Kayrock.Apis.HeartbeatTest do
  @moduledoc """
  Tests for Heartbeat API (V0-V4).

  API Key: 12
  Used to: Keep consumer group membership alive between polls.

  Protocol structure:
  - V0: Basic heartbeat
  - V1+: Adds throttle_time in response
  - V3+: Adds group_instance_id for static membership
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.Heartbeat.V0.Request
    alias Kayrock.Heartbeat.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        generation_id: 1,
        member_id: "member-123"
      }

      expected = <<
        # api_key (12 = Heartbeat)
        0,
        12,
        # api_version (0)
        0,
        0,
        # correlation_id
        0,
        0,
        0,
        0,
        # client_id
        0,
        4,
        "test"::binary,
        # group_id
        0,
        8,
        "my-group"::binary,
        # generation_id
        0,
        0,
        0,
        1,
        # member_id
        0,
        10,
        "member-123"::binary
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    test "deserializes response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (0 = no error)
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
    end

    test "deserializes error response (rebalance in progress)" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code (27 = REBALANCE_IN_PROGRESS)
        0,
        27
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 27
    end
  end

  describe "V1" do
    alias Kayrock.Heartbeat.V1.Request
    alias Kayrock.Heartbeat.V1.Response

    test "serializes request (same as V0)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        generation_id: 5,
        member_id: "member"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 12
      assert api_version == 1
    end

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms (50ms)
        0,
        0,
        0,
        50,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.throttle_time_ms == 50
      assert response.error_code == 0
    end
  end

  describe "V3" do
    alias Kayrock.Heartbeat.V3.Request

    test "serializes request with group_instance_id" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        group_instance_id: "instance-1"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 12
      assert api_version == 3
    end

    test "serializes request with nil group_instance_id" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        group_instance_id: nil
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:heartbeat) do
        module = Module.concat([Kayrock, Heartbeat, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          generation_id: 1,
          member_id: "member"
        }

        fields =
          if version >= 3 do
            Map.put(base, :group_instance_id, nil)
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 12
        assert api_version == version
      end
    end
  end
end
