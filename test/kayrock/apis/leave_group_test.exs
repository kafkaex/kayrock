defmodule Kayrock.Apis.LeaveGroupTest do
  @moduledoc """
  Tests for LeaveGroup API (V0-V5).

  API Key: 13
  Used to: Leave a consumer group, triggering a rebalance.

  Protocol structure:
  - V0: Basic leave with member_id
  - V1+: Adds throttle_time in response
  - V3+: Adds batch member leave (multiple members at once)
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.LeaveGroup.V0.Request
    alias Kayrock.LeaveGroup.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        member_id: "member-123"
      }

      expected = <<
        # api_key (13 = LeaveGroup)
        0,
        13,
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

    test "deserializes error response (unknown member)" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code (25 = UNKNOWN_MEMBER_ID)
        0,
        25
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 25
    end
  end

  describe "V1" do
    alias Kayrock.LeaveGroup.V1.Request
    alias Kayrock.LeaveGroup.V1.Response

    test "serializes request (same as V0)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        member_id: "member"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 13
      assert api_version == 1
    end

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms (50)
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
    alias Kayrock.LeaveGroup.V3.Request

    test "serializes request with batch members" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        members: [
          %{member_id: "member-1", group_instance_id: nil},
          %{member_id: "member-2", group_instance_id: "static-1"}
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 13
      assert api_version == 3
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:leave_group) do
        module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group"
        }

        fields =
          if version >= 3 do
            Map.put(base, :members, [%{member_id: "member", group_instance_id: nil}])
          else
            Map.put(base, :member_id, "member")
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 13
        assert api_version == version
      end
    end
  end
end
