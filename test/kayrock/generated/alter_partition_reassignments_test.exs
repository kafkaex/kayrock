defmodule Kayrock.AlterPartitionReassignmentsTest do
  @moduledoc """
  Tests for AlterPartitionReassignments API (V0).

  API Key: 45
  Used to: Alter partition reassignments (move replicas between brokers).

  Protocol structure:
  - V0: Uses flexible/compact format
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.AlterPartitionReassignmentsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    test "serializes version 0 request to expected binary" do
      {request, expected_binary} = AlterPartitionReassignmentsFactory.request_data(0)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary

      <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
      assert api_key == 45
      assert api_version == 0
      assert correlation_id == request.correlation_id
    end

    test "deserializes version 0 response to expected struct" do
      {response_binary, expected_struct} = AlterPartitionReassignmentsFactory.response_data(0)

      {actual_struct, rest} =
        Kayrock.AlterPartitionReassignments.V0.Response.deserialize(response_binary)

      assert rest == <<>>
      assert actual_struct == expected_struct
      assert actual_struct.correlation_id == 0
      assert is_list(actual_struct.responses)
    end

    test "all available versions have modules" do
      request_module = Kayrock.AlterPartitionReassignments.V0.Request
      response_module = Kayrock.AlterPartitionReassignments.V0.Response

      assert Code.ensure_loaded?(request_module),
             "Request module #{inspect(request_module)} should exist"

      assert Code.ensure_loaded?(response_module),
             "Response module #{inspect(response_module)} should exist"
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic alter partition reassignments" do
    alias Kayrock.AlterPartitionReassignments.V0.Request
    alias Kayrock.AlterPartitionReassignments.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        timeout_ms: 60000,
        topics: [
          %{
            name: "my-topic",
            partitions: [
              %{partition_index: 0, replicas: [1, 2], tagged_fields: []}
            ],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 45
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = AlterPartitionReassignmentsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0
      assert response.error_code == 0

      [resp] = response.responses
      assert resp.name == "topic1"
      [partition] = resp.partitions
      assert partition.partition_index == 0
      assert partition.error_code == 0
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version" do
      request = %Kayrock.AlterPartitionReassignments.V0.Request{
        correlation_id: 0,
        client_id: "test",
        timeout_ms: 30000,
        topics: [],
        tagged_fields: []
      }

      assert Kayrock.Request.api_vsn(request) == 0
    end

    test "response_deserializer returns deserialize function" do
      request = %Kayrock.AlterPartitionReassignments.V0.Request{
        correlation_id: 0,
        client_id: "test",
        timeout_ms: 30000,
        topics: [],
        tagged_fields: []
      }

      deserializer = Kayrock.Request.response_deserializer(request)
      assert is_function(deserializer, 1)
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "V0 response handles truncated binary" do
      {response_binary, _} = AlterPartitionReassignmentsFactory.response_data(0)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.AlterPartitionReassignments.V0.Response,
          response_binary,
          truncate_at
        )
      end
    end
  end

  describe "extra bytes handling" do
    test "V0 response handles extra trailing bytes" do
      {response_binary, _} = AlterPartitionReassignmentsFactory.response_data(0)

      assert_extra_bytes_returned(
        Kayrock.AlterPartitionReassignments.V0.Response,
        response_binary,
        <<99, 88, 77>>
      )
    end
  end

  describe "malformed response handling" do
    test "V0 empty binary fails with MatchError" do
      assert_raise MatchError, fn ->
        Kayrock.AlterPartitionReassignments.V0.Response.deserialize(<<>>)
      end
    end
  end

  describe "error code handling" do
    test "V0 handles NOT_CONTROLLER (58)" do
      response_binary = AlterPartitionReassignmentsFactory.error_response(0, error_code: 58)

      {response, <<>>} =
        Kayrock.AlterPartitionReassignments.V0.Response.deserialize(response_binary)

      assert response.error_code == 58
      assert response.error_message == "Not controller"
    end
  end
end
