defmodule Kayrock.Apis.DescribeGroupsTest do
  @moduledoc """
  Tests for DescribeGroups API (V0-V3).

  API Key: 15
  Used to: Get detailed information about consumer groups.

  Protocol structure:
  - V0: Basic group description
  - V1+: Adds throttle_time in response
  - V3+: Adds include_authorized_operations
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.DescribeGroups.V0.Request
    alias Kayrock.DescribeGroups.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        groups: ["group1", "group2"]
      }

      expected = <<
        # api_key (15 = DescribeGroups)
        0,
        15,
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
        # groups array length
        0,
        0,
        0,
        2,
        # group 1
        0,
        6,
        "group1"::binary,
        # group 2
        0,
        6,
        "group2"::binary
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    test "serializes request with single group" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        groups: ["my-group"]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "deserializes response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # groups array length
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # group_id
        0,
        8,
        "my-group"::binary,
        # state
        0,
        6,
        "Stable"::binary,
        # protocol_type
        0,
        8,
        "consumer"::binary,
        # protocol
        0,
        5,
        "range"::binary,
        # members (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [group] = response.groups
      assert group.error_code == 0
      assert group.group_id == "my-group"
      assert group.group_state == "Stable"
      assert group.protocol_type == "consumer"
      assert group.protocol_data == "range"
    end

    test "deserializes response with members" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # groups
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # group_id
        0,
        5,
        "group"::binary,
        # state
        0,
        6,
        "Stable"::binary,
        # protocol_type
        0,
        8,
        "consumer"::binary,
        # protocol
        0,
        5,
        "range"::binary,
        # members array
        0,
        0,
        0,
        1,
        # member_id
        0,
        6,
        "member"::binary,
        # client_id
        0,
        6,
        "client"::binary,
        # client_host
        0,
        9,
        "localhost"::binary,
        # member_metadata
        0,
        0,
        0,
        4,
        1,
        2,
        3,
        4,
        # member_assignment
        0,
        0,
        0,
        4,
        5,
        6,
        7,
        8
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [group] = response.groups
      [member] = group.members
      assert member.member_id == "member"
      assert member.client_id == "client"
    end

    test "deserializes error response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # groups
        0,
        0,
        0,
        1,
        # error_code (16 = NOT_COORDINATOR)
        0,
        16,
        0,
        5,
        "group"::binary,
        # state (empty)
        0,
        0,
        # protocol_type (empty)
        0,
        0,
        # protocol (empty)
        0,
        0,
        # members (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [group] = response.groups
      assert group.error_code == 16
    end
  end

  describe "V1" do
    alias Kayrock.DescribeGroups.V1.Response

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms
        0,
        0,
        0,
        100,
        # groups
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        0,
        5,
        "group"::binary,
        0,
        5,
        "Empty"::binary,
        # protocol_type
        0,
        0,
        # protocol
        0,
        0,
        # members
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == 100
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:describe_groups) do
        module = Module.concat([Kayrock, DescribeGroups, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          groups: []
        }

        fields =
          if version >= 3 do
            Map.put(base, :include_authorized_operations, false)
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 15
        assert api_version == version
      end
    end
  end
end
