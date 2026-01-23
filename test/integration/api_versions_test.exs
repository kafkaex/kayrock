defmodule Kayrock.Integration.ApiVersionsTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "ApiVersions API" do
    for api_version <- [0, 1, 2] do
      test "v#{api_version} - returns supported API versions", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        request = api_versions_request(api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :any)

        # Check response structure
        assert response.error_code == 0
        assert is_list(response.api_keys)
        assert length(response.api_keys) > 0

        # Verify common APIs are present
        api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)

        # Produce API (0)
        assert Map.has_key?(api_map, 0)
        produce = api_map[0]
        assert produce.min_version == 0
        assert produce.max_version >= 3

        # Fetch API (1)
        assert Map.has_key?(api_map, 1)
        fetch = api_map[1]
        assert fetch.min_version == 0
        assert fetch.max_version >= 4

        # Metadata API (3)
        assert Map.has_key?(api_map, 3)
        metadata = api_map[3]
        assert metadata.min_version == 0
        assert metadata.max_version >= 1

        # ApiVersions API (18)
        assert Map.has_key?(api_map, 18)
        api_versions = api_map[18]
        assert api_versions.min_version == 0
        assert api_versions.max_version >= api_version
      end

      test "v#{api_version} - returns consumer group APIs", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        request = api_versions_request(api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :any)

        api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)

        # FindCoordinator (10)
        assert Map.has_key?(api_map, 10)

        # JoinGroup (11)
        assert Map.has_key?(api_map, 11)

        # Heartbeat (12)
        assert Map.has_key?(api_map, 12)

        # LeaveGroup (13)
        assert Map.has_key?(api_map, 13)

        # SyncGroup (14)
        assert Map.has_key?(api_map, 14)

        # DescribeGroups (15)
        assert Map.has_key?(api_map, 15)

        # ListGroups (16)
        assert Map.has_key?(api_map, 16)
      end

      test "v#{api_version} - returns admin APIs", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        request = api_versions_request(api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :any)

        api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)

        # CreateTopics (19)
        assert Map.has_key?(api_map, 19)

        # DeleteTopics (20)
        assert Map.has_key?(api_map, 20)

        # DescribeConfigs (32)
        assert Map.has_key?(api_map, 32)

        # AlterConfigs (33)
        assert Map.has_key?(api_map, 33)
      end
    end

    test "v1+ includes throttle_time_ms", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      request = api_versions_request(1)
      {:ok, response} = Kayrock.client_call(client_pid, request, :any)

      assert response.error_code == 0
      assert is_integer(response.throttle_time_ms)
      assert response.throttle_time_ms >= 0
    end

    test "can negotiate API version based on broker support", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      # Get supported versions
      request = api_versions_request(0)
      {:ok, response} = Kayrock.client_call(client_pid, request, :any)

      api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)

      # Get Produce API supported range
      produce_api = api_map[0]
      supported_produce_max = produce_api.max_version

      # Verify we can use the max supported version
      assert supported_produce_max >= Kayrock.Produce.min_vsn()
    end

    test "returns all offset-related APIs", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)

      request = api_versions_request(0)
      {:ok, response} = Kayrock.client_call(client_pid, request, :any)

      api_map = Map.new(response.api_keys, fn api -> {api.api_key, api} end)

      # ListOffsets (2)
      assert Map.has_key?(api_map, 2)
      list_offsets = api_map[2]
      assert list_offsets.min_version == 0

      # OffsetCommit (8)
      assert Map.has_key?(api_map, 8)

      # OffsetFetch (9)
      assert Map.has_key?(api_map, 9)
    end
  end

  defp build_client(kafka) do
    uris = [{"localhost", Container.mapped_port(kafka, 9092)}]
    Kayrock.Client.start_link(uris)
  end
end
