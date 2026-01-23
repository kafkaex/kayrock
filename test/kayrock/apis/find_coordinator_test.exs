defmodule Kayrock.Apis.FindCoordinatorTest do
  @moduledoc """
  Tests for FindCoordinator API (V0-V3).

  API Key: 10
  Used to: Find the coordinator (broker) for a consumer group or transaction.

  Protocol structure:
  - V0: Basic request with group key
  - V1+: Adds key_type (0=group, 1=transaction), throttle_time in response
  - V3: Compact format
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.FindCoordinator.V0.Request
    alias Kayrock.FindCoordinator.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        key: "my-consumer-group"
      }

      expected = <<
        # api_key (10 = FindCoordinator)
        0,
        10,
        # api_version (0)
        0,
        0,
        # correlation_id
        0,
        0,
        0,
        0,
        # client_id length
        0,
        4,
        # client_id
        "test"::binary,
        # key length
        0,
        17,
        # key (group_id)
        "my-consumer-group"::binary
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
        0,
        # node_id (1)
        0,
        0,
        0,
        1,
        # host length
        0,
        9,
        # host
        "localhost"::binary,
        # port (9092)
        0,
        0,
        35,
        132
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert response.node_id == 1
      assert response.host == "localhost"
      assert response.port == 9092
    end

    test "deserializes error response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code (15 = COORDINATOR_NOT_AVAILABLE)
        0,
        15,
        # node_id
        0,
        0,
        0,
        0,
        # empty host
        0,
        0,
        # port 0
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.error_code == 15
      assert response.node_id == 0
    end
  end

  describe "V1" do
    alias Kayrock.FindCoordinator.V1.Request
    alias Kayrock.FindCoordinator.V1.Response

    test "serializes request with key_type" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        key: "group",
        # 0 = group coordinator
        key_type: 0
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 10
      assert api_version == 1
    end

    test "serializes request for transaction coordinator" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        key: "my-txn-id",
        # 1 = transaction coordinator
        key_type: 1
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 10
      assert api_version == 1
    end

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms (100)
        0,
        0,
        0,
        100,
        # error_code
        0,
        0,
        # error_message (null)
        255,
        255,
        # node_id
        0,
        0,
        0,
        2,
        # host
        0,
        9,
        "localhost"::binary,
        # port (9093)
        0,
        0,
        35,
        133
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.throttle_time_ms == 100
      assert response.error_code == 0
      assert response.node_id == 2
      assert response.port == 9093
    end
  end

  describe "V2" do
    alias Kayrock.FindCoordinator.V2.Request

    test "serializes request (same as V1)" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        key: "group",
        key_type: 0
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 10
      assert api_version == 2
    end
  end

  describe "V3" do
    alias Kayrock.FindCoordinator.V3.Request

    test "serializes request with compact format" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        key: "group",
        key_type: 0
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 10
      assert api_version == 3
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:find_coordinator) do
        module = Module.concat([Kayrock, FindCoordinator, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{correlation_id: version, client_id: "test", key: "group"}
        fields = if version >= 1, do: Map.put(base, :key_type, 0), else: base

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 10
        assert api_version == version
      end
    end
  end
end
