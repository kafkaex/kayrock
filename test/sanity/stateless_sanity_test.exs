defmodule Kayrock.Sanity.StatelessSanityTest do
  @moduledoc """
  Sanity tests for stateless APIs — only a broker connection required.

  Covers:
  - ApiVersions V0-V3 (API key 18)
  - Metadata V0-V9 (API key 3)
  - ListGroups V0-V3 (API key 16)

  Run with:
      mix test.sanity test/sanity/stateless_sanity_test.exs
  """
  use Kayrock.SanityCase
  use ExUnit.Case, async: false

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------------

  # Build a minimal valid ApiVersions request for the given version.
  defp build_api_versions_request(version) do
    request = api_versions_request(version)

    if version >= 3 do
      %{
        request
        | client_software_name: "kayrock-sanity",
          client_software_version: "test",
          tagged_fields: []
      }
    else
      request
    end
  end

  # Build a minimal valid Metadata request for the given version.
  # Uses an empty topic list (all versions except V0 support nil; V0 requires []).
  defp build_metadata_request(version) do
    # V0: topics must be a non-nil list (nil/all-topics added in V1+)
    topics = if version == 0, do: [], else: nil

    request = metadata_request(topics, version)

    cond do
      version >= 9 ->
        %{
          request
          | tagged_fields: [],
            include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false
        }

      version >= 8 ->
        %{
          request
          | include_cluster_authorized_operations: false,
            include_topic_authorized_operations: false
        }

      true ->
        request
    end
  end

  # Build a minimal valid ListGroups request for the given version.
  defp build_list_groups_request(version) do
    request = list_consumer_groups_request(version)

    if version >= 3 do
      %{request | tagged_fields: []}
    else
      request
    end
  end

  # ---------------------------------------------------------------------------
  # ApiVersions V0-V3
  # ---------------------------------------------------------------------------

  describe "ApiVersions" do
    # V3 provides unique flexible-header coverage (KIP-482/KIP-511).
    # V0-V2 basic success tests are covered by test/integration/api_versions_test.exs.
    @tag api: :api_versions, version: 3
    test "V3 returns success and parseable response", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      request = build_api_versions_request(3)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert response.error_code == 0,
             "V3 expected error_code 0, got #{response.error_code}"

      assert is_list(response.api_keys),
             "V3 api_keys should be a list"

      assert response.api_keys != [],
             "V3 broker should report at least one supported API"
    end

    for version <- 0..3 do
      @tag api: :api_versions, version: version
      test "V#{version} api_keys contain valid entries", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_api_versions_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        for api_entry <- response.api_keys do
          assert is_integer(api_entry.api_key),
                 "V#{version}: api_entry.api_key should be integer, got #{inspect(api_entry.api_key)}"

          assert is_integer(api_entry.min_version),
                 "V#{version}: api_entry.min_version should be integer"

          assert is_integer(api_entry.max_version),
                 "V#{version}: api_entry.max_version should be integer"

          assert api_entry.max_version >= api_entry.min_version,
                 "V#{version}: max_version (#{api_entry.max_version}) >= min_version (#{api_entry.min_version})"
        end
      end
    end

    # V0 has no throttle_time_ms field
    @tag api: :api_versions, version: 0
    test "V0 response has no throttle_time_ms field", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_api_versions_request(0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 response should not have throttle_time_ms"
    end

    # V1/V2/V3 include throttle_time_ms
    for version <- 1..3 do
      @tag api: :api_versions, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_api_versions_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be non-negative"
      end
    end

    # V3 is flexible — response must also include tagged_fields
    @tag api: :api_versions, version: 3
    test "V3 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_api_versions_request(3)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V3 response.tagged_fields should be a list (flexible version)"
    end

    # KIP-511: response header always uses v0 (no tagged_fields in header)
    # regardless of request version. A wrong header parser would crash here.
    @tag api: :api_versions
    test "V3 request/response header round-trip per KIP-511", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_api_versions_request(3)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert response.error_code == 0
    end

    # The broker must report ApiVersions (key 18) in its own response
    @tag api: :api_versions
    test "broker reports ApiVersions API (key 18) in V1 response", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_api_versions_request(1)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      api_map = Map.new(response.api_keys, &{&1.api_key, &1})
      assert Map.has_key?(api_map, 18), "broker should report ApiVersions (key 18)"
    end
  end

  # ---------------------------------------------------------------------------
  # Metadata V0-V9
  # ---------------------------------------------------------------------------

  describe "Metadata" do
    for version <- 0..9 do
      @tag api: :metadata, version: version
      test "V#{version} returns success and parseable response", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_metadata_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_list(response.brokers),
               "V#{version} brokers should be a list"

        assert response.brokers != [],
               "V#{version} broker list should not be empty"
      end

      @tag api: :metadata, version: version
      test "V#{version} broker entries have required fields", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_metadata_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        for broker <- response.brokers do
          assert is_integer(broker.node_id),
                 "V#{version}: broker.node_id should be integer"

          assert is_binary(broker.host),
                 "V#{version}: broker.host should be binary"

          assert is_integer(broker.port),
                 "V#{version}: broker.port should be integer"

          assert broker.port > 0,
                 "V#{version}: broker.port should be > 0, got #{broker.port}"
        end
      end
    end

    # V0 does not have cluster_id or controller_id
    @tag api: :metadata, version: 0
    test "V0 response has topics list", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_metadata_request(0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.topics)
    end

    # V1+ include controller_id
    for version <- 1..9 do
      @tag api: :metadata, version: version
      test "V#{version} response includes controller_id", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_metadata_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.controller_id),
               "V#{version} controller_id should be integer"

        assert response.controller_id >= 0,
               "V#{version} controller_id should be >= 0, got #{response.controller_id}"
      end
    end

    # V2+ include cluster_id
    for version <- 2..9 do
      @tag api: :metadata, version: version
      test "V#{version} response includes cluster_id", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_metadata_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_binary(response.cluster_id) or is_nil(response.cluster_id),
               "V#{version} cluster_id should be binary or nil"
      end
    end

    # V9 is flexible — response must include tagged_fields
    @tag api: :metadata, version: 9
    test "V9 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_metadata_request(9)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V9 response.tagged_fields should be a list (flexible version)"
    end

    # V8+ include cluster_authorized_operations in response
    for version <- 8..9 do
      @tag api: :metadata, version: version
      test "V#{version} response includes cluster_authorized_operations", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_metadata_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.cluster_authorized_operations),
               "V#{version} cluster_authorized_operations should be integer"
      end
    end

    # V1+ allow nil topics (all-topics request)
    for version <- 1..9 do
      @tag api: :metadata, version: version
      test "V#{version} nil topics (all-topics) returns broker list", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = metadata_request(nil, version)

        request =
          cond do
            version >= 9 ->
              %{
                request
                | tagged_fields: [],
                  include_cluster_authorized_operations: false,
                  include_topic_authorized_operations: false
              }

            version >= 8 ->
              %{
                request
                | include_cluster_authorized_operations: false,
                  include_topic_authorized_operations: false
              }

            true ->
              request
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_list(response.brokers), "V#{version} nil-topics brokers should be a list"
        assert is_list(response.topics), "V#{version} nil-topics topics should be a list"
      end
    end
  end

  # ---------------------------------------------------------------------------
  # ListGroups V0-V3
  # ---------------------------------------------------------------------------

  describe "ListGroups" do
    for version <- 0..3 do
      @tag api: :list_groups, version: version
      test "V#{version} returns success and parseable response", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_list_groups_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert response.error_code == 0,
               "V#{version} expected error_code 0, got #{response.error_code}"

        assert is_list(response.groups),
               "V#{version} groups should be a list (may be empty)"
      end
    end

    # V0 has no throttle_time_ms
    @tag api: :list_groups, version: 0
    test "V0 response has no throttle_time_ms field", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_list_groups_request(0)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      refute Map.has_key?(response, :throttle_time_ms),
             "V0 response should not have throttle_time_ms"
    end

    # V1/V2/V3 include throttle_time_ms
    for version <- 1..3 do
      @tag api: :list_groups, version: version
      test "V#{version} response includes throttle_time_ms", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = build_list_groups_request(version)
        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be non-negative"
      end
    end

    # V3 is flexible — response must include tagged_fields
    @tag api: :list_groups, version: 3
    test "V3 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_list_groups_request(3)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      assert is_list(response.tagged_fields),
             "V3 response.tagged_fields should be a list (flexible version)"
    end

    # When groups exist, verify their structure
    @tag api: :list_groups
    test "V2 groups entries have group_id and protocol_type when groups exist", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      request = build_list_groups_request(2)
      {:ok, response} = Kayrock.client_call(client, request, :random)

      for group <- response.groups do
        assert is_binary(group.group_id),
               "group_id should be a binary string"

        assert is_binary(group.protocol_type),
               "protocol_type should be a binary string"
      end
    end
  end
end
