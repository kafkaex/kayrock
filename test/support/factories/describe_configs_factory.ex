# credo:disable-for-this-file Credo.Check.Refactor.Nesting
defmodule Kayrock.Test.Factories.DescribeConfigsFactory do
  @moduledoc """
  Factory for generating DescribeConfigs API test data (V0-V2).

  API Key: 32
  Used to: Get configuration for topics, brokers, and other resources.

  Protocol changes by version:
  - V0: Basic config description (resources with config_entries)
  - V1+: Adds include_synonyms flag in request, config_source and config_synonyms in response
  - V2+: Adds include_documentation flag in request (response schema same as V1)
  """

  # ============================================
  # Primary API - Request Data
  # ============================================

  @doc """
  Returns request test data for the specified version.

  Returns a tuple of `{struct, expected_binary}` where:
  - `struct` is the Request struct to serialize
  - `expected_binary` is the expected serialized output

  ## Examples

      iex> {request, expected_binary} = request_data(0)
      iex> serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      iex> assert serialized == expected_binary
  """
  def request_data(version)

  def request_data(0) do
    request = %Kayrock.DescribeConfigs.V0.Request{
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

    expected_binary = <<
      # api_key (32 = DescribeConfigs)
      0,
      32,
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
      # resources array length (1)
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name length
      0,
      8,
      # resource_name
      "my-topic"::binary,
      # config_names (null = -1)
      255,
      255,
      255,
      255
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DescribeConfigs.V1.Request{
      correlation_id: 1,
      client_id: "test",
      resources: [
        %{
          resource_type: 2,
          resource_name: "topic",
          config_names: ["retention.ms"]
        }
      ],
      include_synonyms: true
    }

    expected_binary = <<
      # api_key (32)
      0,
      32,
      # api_version (1)
      0,
      1,
      # correlation_id
      0,
      0,
      0,
      1,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary,
      # resources array length (1)
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name length
      0,
      5,
      # resource_name
      "topic"::binary,
      # config_names array length (1)
      0,
      0,
      0,
      1,
      # config_name length
      0,
      12,
      # config_name
      "retention.ms"::binary,
      # include_synonyms (true = 1)
      1
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.DescribeConfigs.V2.Request{
      correlation_id: 2,
      client_id: "test",
      resources: [
        %{
          resource_type: 4,
          resource_name: "0",
          config_names: nil
        }
      ],
      include_synonyms: false
    }

    expected_binary = <<
      # api_key (32)
      0,
      32,
      # api_version (2)
      0,
      2,
      # correlation_id
      0,
      0,
      0,
      2,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary,
      # resources array length (1)
      0,
      0,
      0,
      1,
      # resource_type (4 = BROKER)
      4,
      # resource_name length
      0,
      1,
      # resource_name
      "0"::binary,
      # config_names (null = -1)
      255,
      255,
      255,
      255,
      # include_synonyms (false = 0)
      0
    >>

    {request, expected_binary}
  end

  # ============================================
  # Primary API - Response Data
  # ============================================

  @doc """
  Returns response test data for the specified version.

  Returns a tuple of `{binary, expected_struct}` where:
  - `binary` is the response binary to deserialize
  - `expected_struct` is the expected deserialized struct

  ## Examples

      iex> {response_binary, expected_struct} = response_data(0)
      iex> {actual, <<>>} = Kayrock.DescribeConfigs.V0.Response.deserialize(response_binary)
      iex> assert actual == expected_struct
  """
  def response_data(version)

  def response_data(0) do
    binary = <<
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
      # resources array length (1)
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
      # resource_name length
      0,
      5,
      # resource_name
      "topic"::binary,
      # config_entries array length (1)
      0,
      0,
      0,
      1,
      # config_name length
      0,
      12,
      # config_name
      "retention.ms"::binary,
      # config_value length
      0,
      8,
      # config_value
      "86400000"::binary,
      # read_only (false)
      0,
      # is_default (false)
      0,
      # is_sensitive (false)
      0
    >>

    expected_struct = %Kayrock.DescribeConfigs.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      resources: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic",
          config_entries: [
            %{
              config_name: "retention.ms",
              config_value: "86400000",
              read_only: 0,
              is_default: 0,
              is_sensitive: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    binary = <<
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
      # resources array length (1)
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
      # resource_name length
      0,
      5,
      # resource_name
      "topic"::binary,
      # config_entries array length (1)
      0,
      0,
      0,
      1,
      # config_name length
      0,
      12,
      # config_name
      "retention.ms"::binary,
      # config_value length
      0,
      8,
      # config_value
      "86400000"::binary,
      # read_only (false)
      0,
      # config_source (5 = DEFAULT_CONFIG)
      5,
      # is_sensitive (false)
      0,
      # config_synonyms array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeConfigs.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      resources: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic",
          config_entries: [
            %{
              config_name: "retention.ms",
              config_value: "86400000",
              read_only: 0,
              config_source: 5,
              is_sensitive: 0,
              config_synonyms: []
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      2,
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # resources array length (1)
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
      # resource_type (4 = BROKER)
      4,
      # resource_name length
      0,
      1,
      # resource_name
      "0"::binary,
      # config_entries array length (1)
      0,
      0,
      0,
      1,
      # config_name length
      0,
      8,
      # config_name
      "log.dirs"::binary,
      # config_value length
      0,
      9,
      # config_value
      "/tmp/logs"::binary,
      # read_only (false)
      0,
      # config_source (1 = DYNAMIC_BROKER_CONFIG)
      1,
      # is_sensitive (false)
      0,
      # config_synonyms array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeConfigs.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      resources: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 4,
          resource_name: "0",
          config_entries: [
            %{
              config_name: "log.dirs",
              config_value: "/tmp/logs",
              read_only: 0,
              config_source: 1,
              is_sensitive: 0,
              config_synonyms: []
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary for the specified version with options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:throttle_time_ms` - Default: 0
  - `:resources` - List of resource results (default: single success resource)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    resources =
      Keyword.get(opts, :resources, [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic",
          config_entries: []
        }
      ])

    resources_binary =
      for resource <- resources, into: <<>> do
        error_code = Map.get(resource, :error_code, 0)
        error_message = Map.get(resource, :error_message, nil)
        resource_type = Map.get(resource, :resource_type, 2)
        resource_name = Map.get(resource, :resource_name, "topic")
        config_entries = Map.get(resource, :config_entries, [])

        error_msg_binary =
          case error_message do
            nil -> <<-1::16-signed>>
            msg -> <<byte_size(msg)::16, msg::binary>>
          end

        configs_binary =
          for config <- config_entries, into: <<>> do
            name = Map.get(config, :config_name, "key")
            value = Map.get(config, :config_value, "value")
            read_only = Map.get(config, :read_only, 0)
            is_default = Map.get(config, :is_default, 0)
            is_sensitive = Map.get(config, :is_sensitive, 0)

            value_binary =
              case value do
                nil -> <<-1::16-signed>>
                v -> <<byte_size(v)::16, v::binary>>
              end

            <<
              byte_size(name)::16,
              name::binary,
              value_binary::binary,
              read_only,
              is_default,
              is_sensitive
            >>
          end

        <<
          error_code::16-signed,
          error_msg_binary::binary,
          resource_type,
          byte_size(resource_name)::16,
          resource_name::binary,
          length(config_entries)::32,
          configs_binary::binary
        >>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      length(resources)::32,
      resources_binary::binary
    >>
  end

  def response_binary(version, opts) when version in [1, 2] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    resources =
      Keyword.get(opts, :resources, [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic",
          config_entries: []
        }
      ])

    resources_binary =
      for resource <- resources, into: <<>> do
        error_code = Map.get(resource, :error_code, 0)
        error_message = Map.get(resource, :error_message, nil)
        resource_type = Map.get(resource, :resource_type, 2)
        resource_name = Map.get(resource, :resource_name, "topic")
        config_entries = Map.get(resource, :config_entries, [])

        error_msg_binary =
          case error_message do
            nil -> <<-1::16-signed>>
            msg -> <<byte_size(msg)::16, msg::binary>>
          end

        configs_binary =
          for config <- config_entries, into: <<>> do
            name = Map.get(config, :config_name, "key")
            value = Map.get(config, :config_value, "value")
            read_only = Map.get(config, :read_only, 0)
            config_source = Map.get(config, :config_source, 5)
            is_sensitive = Map.get(config, :is_sensitive, 0)
            synonyms = Map.get(config, :config_synonyms, [])

            value_binary =
              case value do
                nil -> <<-1::16-signed>>
                v -> <<byte_size(v)::16, v::binary>>
              end

            synonyms_binary =
              for synonym <- synonyms, into: <<>> do
                syn_name = Map.get(synonym, :config_name, "key")
                syn_value = Map.get(synonym, :config_value, nil)
                syn_source = Map.get(synonym, :config_source, 5)

                syn_value_binary =
                  case syn_value do
                    nil -> <<-1::16-signed>>
                    v -> <<byte_size(v)::16, v::binary>>
                  end

                <<
                  byte_size(syn_name)::16,
                  syn_name::binary,
                  syn_value_binary::binary,
                  syn_source
                >>
              end

            <<
              byte_size(name)::16,
              name::binary,
              value_binary::binary,
              read_only,
              config_source,
              is_sensitive,
              length(synonyms)::32,
              synonyms_binary::binary
            >>
          end

        <<
          error_code::16-signed,
          error_msg_binary::binary,
          resource_type,
          byte_size(resource_name)::16,
          resource_name::binary,
          length(config_entries)::32,
          configs_binary::binary
        >>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      length(resources)::32,
      resources_binary::binary
    >>
  end

  @doc """
  Builds a minimal valid response (empty resources).
  """
  def minimal_response(version, opts \\ []) do
    response_binary(version, Keyword.put(opts, :resources, []))
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 40 (INVALID_CONFIG)
  - `:error_message` - Error message (default: "Invalid config")
  - `:resource_type` - Default: 2 (TOPIC)
  - `:resource_name` - Default: "topic"
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 40)
    error_message = Keyword.get(opts, :error_message, "Invalid config")
    resource_type = Keyword.get(opts, :resource_type, 2)
    resource_name = Keyword.get(opts, :resource_name, "topic")

    resource = %{
      error_code: error_code,
      error_message: error_message,
      resource_type: resource_type,
      resource_name: resource_name,
      config_entries: []
    }

    response_binary(version, Keyword.put(opts, :resources, [resource]))
  end

  # ============================================
  # Legacy API (backward compatibility)
  # ============================================

  @doc """
  Returns just the captured request binary for the specified version.
  """
  def captured_request_binary(version) do
    {_struct, binary} = request_data(version)
    binary
  end

  @doc """
  Returns just the captured response binary for the specified version.
  """
  def captured_response_binary(version) do
    {binary, _struct} = response_data(version)
    binary
  end
end
