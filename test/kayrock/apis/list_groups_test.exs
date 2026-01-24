defmodule Kayrock.Apis.ListGroupsTest do
  @moduledoc """
  Tests for ListGroups API (V0-V3).

  API Key: 16
  Used to: List all consumer groups on a broker.

  Protocol structure:
  - V0: Basic group listing
  - V1+: Adds throttle_time in response
  - V3+: Compact format with tagged_fields
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.ListGroups.V0.Request
    alias Kayrock.ListGroups.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test"
      }

      expected = <<
        # api_key (16 = ListGroups)
        0,
        16,
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
        "test"::binary
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
        # error_code
        0,
        0,
        # groups array length
        0,
        0,
        0,
        2,
        # group_id
        0,
        6,
        "group1"::binary,
        # protocol_type
        0,
        8,
        "consumer"::binary,
        # group_id
        0,
        6,
        "group2"::binary,
        # protocol_type
        0,
        8,
        "consumer"::binary
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert length(response.groups) == 2

      [group1, group2] = response.groups
      assert group1.group_id == "group1"
      assert group1.protocol_type == "consumer"
      assert group2.group_id == "group2"
    end

    test "deserializes empty groups" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # empty groups array
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.error_code == 0
      assert response.groups == []
    end

    test "deserializes error response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # error_code (29 = COORDINATOR_LOAD_IN_PROGRESS)
        0,
        29,
        # groups
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 29
    end
  end

  describe "V1" do
    alias Kayrock.ListGroups.V1.Response

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
        50,
        # error_code
        0,
        0,
        # groups
        0,
        0,
        0,
        1,
        0,
        5,
        "group"::binary,
        0,
        8,
        "consumer"::binary
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == 50
      [group] = response.groups
      assert group.group_id == "group"
    end
  end

  describe "V3" do
    alias Kayrock.ListGroups.V3.Request

    test "serializes request with compact format" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        tagged_fields: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 16
      assert api_version == 3
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:list_groups) do
        module = Module.concat([Kayrock, ListGroups, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test"
        }

        fields =
          if version >= 3 do
            Map.put(base, :tagged_fields, [])
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 16
        assert api_version == version
      end
    end
  end
end
