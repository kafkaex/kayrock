defmodule Kayrock.Integration.FlexibleHeaderIntegrationTest do
  @moduledoc """
  Integration tests verifying that flexible-version requests (KIP-482) are
  accepted by a real Kafka broker.

  Background: the code generator was encoding client_id as a
  compact_nullable_string (varint-length) in flexible-version request headers,
  but the Kafka protocol requires int16-prefixed nullable_string for client_id
  in ALL header versions. The broker rejected the malformed frames with
  `BufferUnderflowException`. These tests ensure the fix is correct end-to-end.

  Additionally, ApiVersions responses always use header v0 (no tagged_fields)
  per KIP-511, regardless of the request version. A second generator fix
  handles that exception.
  """
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "ApiVersions V3 (flexible header)" do
    test "request succeeds and response parses correctly", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      # V3 is the first flexible version for ApiVersions
      # It includes client_software_name/version in the request body
      request = api_versions_request(3)

      request = %{
        request
        | client_software_name: "kayrock",
          client_software_version: "1.0.0-test",
          tagged_fields: []
      }

      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      # Response must parse without error
      assert response.error_code == 0
      assert is_list(response.api_keys)
      assert response.api_keys != [], "broker should report supported APIs"

      # V3 response includes throttle_time_ms
      assert is_integer(response.throttle_time_ms)
      assert response.throttle_time_ms >= 0

      # Verify the broker reports ApiVersions (18) support up to at least V3
      api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)
      assert Map.has_key?(api_map, 18)
      assert api_map[18].max_version >= 3
    end

    test "response header uses v0 (no tagged_fields) per KIP-511", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      request = api_versions_request(3)

      request = %{
        request
        | client_software_name: "kayrock",
          client_software_version: "1.0.0-test",
          tagged_fields: []
      }

      # If the response header parser incorrectly expects tagged_fields,
      # this call would fail with a deserialization error
      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      assert response.error_code == 0
      assert is_integer(response.correlation_id)
    end
  end

  describe "Metadata V9 (flexible header)" do
    test "request succeeds with nil topics (all topics)", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, 3)

      request = metadata_request(nil, 9)

      request = %{
        request
        | tagged_fields: [],
          include_cluster_authorized_operations: false,
          include_topic_authorized_operations: false
      }

      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      topic_names = Enum.map(response.topics, & &1.name)
      assert topic_name in topic_names
    end

    test "response includes cluster_id and controller_id", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      request = metadata_request([], 9)

      request = %{
        request
        | tagged_fields: [],
          include_cluster_authorized_operations: false,
          include_topic_authorized_operations: false
      }

      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      assert is_binary(response.cluster_id) or is_nil(response.cluster_id)
      assert is_integer(response.controller_id)
      assert response.controller_id >= 0
    end
  end

  describe "FindCoordinator V3 (flexible header)" do
    test "request is accepted by broker", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      group_id = "flex-header-test-#{unique_string()}"
      request = find_coordinator_request(group_id, 3)
      request = %{request | tagged_fields: []}

      # The broker may return COORDINATOR_NOT_AVAILABLE (15) for a brand-new
      # group, which is fine -- it means the request was parsed successfully.
      # A header encoding bug would cause a connection close / TCP error instead.
      {:ok, response} =
        with_retry(
          fn -> Kayrock.client_call(client_pid, request, :random) end,
          accept_errors: [15]
        )

      assert response.error_code in [0, 15]
      assert is_integer(response.correlation_id)

      if response.error_code == 0 do
        assert is_integer(response.node_id)
        assert is_binary(response.host)
        assert is_integer(response.port)
        assert response.port > 0
      end
    end

    test "response includes throttle_time_ms", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      group_id = "flex-header-throttle-#{unique_string()}"
      request = find_coordinator_request(group_id, 3)
      request = %{request | tagged_fields: []}

      {:ok, response} =
        with_retry(
          fn -> Kayrock.client_call(client_pid, request, :random) end,
          accept_errors: [15]
        )

      assert is_integer(response.throttle_time_ms)
      assert response.throttle_time_ms >= 0
    end
  end

  describe "Heartbeat V4 (flexible header)" do
    test "request is parseable by broker (returns protocol-level error, not connection close)",
         %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      # Build V4 request manually -- no active consumer group, so the broker
      # will return an error code. The key assertion is that we get a proper
      # Kafka error response rather than a TCP disconnect or crash.
      request = Kayrock.Heartbeat.get_request_struct(4)

      request = %{
        request
        | group_id: "nonexistent-heartbeat-group",
          generation_id: 0,
          member_id: "",
          group_instance_id: nil,
          tagged_fields: []
      }

      # Use with_retry because coordinator may not be ready immediately
      {:ok, response} =
        with_retry(
          fn -> Kayrock.client_call(client_pid, request, :random) end,
          accept_errors: [
            # COORDINATOR_NOT_AVAILABLE
            15,
            # NOT_COORDINATOR
            16,
            # UNKNOWN_MEMBER_ID
            25,
            # REBALANCE_IN_PROGRESS
            27
          ]
        )

      # Any error code is acceptable -- the point is we got a parsed response
      assert is_integer(response.error_code)
      assert is_integer(response.correlation_id)

      # V4 response includes throttle_time_ms
      assert is_integer(response.throttle_time_ms)
      assert response.throttle_time_ms >= 0
    end

    test "V4 heartbeat works for an active group member", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, 3)
      group_id = "heartbeat-v4-flex-#{unique_string()}"

      {node_id, generation_id, member_id} =
        join_and_sync_group(client_pid, group_id, topic_name, api_version: 5)

      # Send heartbeat V4 (flexible version) -- this is the actual test
      request = Kayrock.Heartbeat.get_request_struct(4)

      request = %{
        request
        | group_id: group_id,
          generation_id: generation_id,
          member_id: member_id,
          group_instance_id: nil,
          tagged_fields: []
      }

      {:ok, heartbeat_response} = Kayrock.client_call(client_pid, request, node_id)

      assert heartbeat_response.error_code == 0
      assert is_integer(heartbeat_response.throttle_time_ms)
    end
  end
end
