defmodule Kayrock.Apis.DescribeConfigsTest do
  @moduledoc """
  Tests for DescribeConfigs API (V0-V2).

  API Key: 32
  Used to: Get configuration for topics, brokers, and other resources.

  Protocol structure:
  - V0: Basic config description
  - V1+: Adds include_synonyms flag
  - V2+: Adds include_documentation flag
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.DescribeConfigs.V0.Request
    alias Kayrock.DescribeConfigs.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        resources: [
          %{
            # 2 = TOPIC
            resource_type: 2,
            resource_name: "my-topic",
            config_names: nil
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 32
      assert api_version == 0
    end

    test "serializes request for broker config" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        resources: [
          %{
            # 4 = BROKER
            resource_type: 4,
            resource_name: "0",
            config_names: nil
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "serializes request with specific config names" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        resources: [
          %{
            resource_type: 2,
            resource_name: "topic",
            config_names: ["retention.ms", "cleanup.policy"]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "deserializes response" do
      # V0 response has throttle_time_ms before the resources array
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # results array length
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # error_message (null)
        255,
        255,
        # resource_type (2 = TOPIC)
        2,
        # resource_name
        0,
        5,
        "topic"::binary,
        # configs array
        0,
        0,
        0,
        1,
        # config name
        0,
        12,
        "retention.ms"::binary,
        # config value
        0,
        8,
        "86400000"::binary,
        # read_only (false)
        0,
        # is_default (false)
        0,
        # is_sensitive (false)
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [resource] = response.resources
      assert resource.error_code == 0
      assert resource.resource_type == 2
      assert resource.resource_name == "topic"
      [config] = resource.config_entries
      assert config.config_name == "retention.ms"
      assert config.config_value == "86400000"
    end
  end

  describe "V1" do
    alias Kayrock.DescribeConfigs.V1.Request

    test "serializes request with include_synonyms" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        resources: [],
        include_synonyms: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 32
      assert api_version == 1
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:describe_configs) do
        module = Module.concat([Kayrock, DescribeConfigs, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          resources: []
        }

        fields =
          cond do
            version >= 2 ->
              Map.merge(base, %{include_synonyms: false, include_documentation: false})

            version >= 1 ->
              Map.put(base, :include_synonyms, false)

            true ->
              base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 32
        assert api_version == version
      end
    end
  end
end
