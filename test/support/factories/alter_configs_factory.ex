defmodule Kayrock.Test.Factories.AlterConfigsFactory do
  @moduledoc """
  Factory for AlterConfigs API test data (V0-V1).

  API Key: 33
  Used to: Alter configuration for resources (topics, brokers).

  Protocol structure:
  - V0-V1: Same schema - resources with config_entries
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.AlterConfigs.V0.Request{
      correlation_id: 0,
      client_id: "test",
      resources: [
        %{
          resource_type: 2,
          resource_name: "topic1",
          config_entries: [
            %{config_name: "retention.ms", config_value: "86400000"}
          ]
        }
      ],
      validate_only: false
    }

    expected_binary = <<
      # api_key
      0,
      33,
      # api_version
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
      # resources array length
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name
      0,
      6,
      "topic1"::binary,
      # config_entries array length
      0,
      0,
      0,
      1,
      # config_name
      0,
      12,
      "retention.ms"::binary,
      # config_value
      0,
      8,
      "86400000"::binary,
      # validate_only
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.AlterConfigs.V1.Request{
      correlation_id: 1,
      client_id: "test",
      resources: [
        %{
          resource_type: 2,
          resource_name: "topic1",
          config_entries: [
            %{config_name: "retention.ms", config_value: "172800000"}
          ]
        }
      ],
      validate_only: true
    }

    expected_binary = <<
      # api_key
      0,
      33,
      # api_version
      0,
      1,
      # correlation_id
      0,
      0,
      0,
      1,
      # client_id
      0,
      4,
      "test"::binary,
      # resources array length
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name
      0,
      6,
      "topic1"::binary,
      # config_entries array length
      0,
      0,
      0,
      1,
      # config_name
      0,
      12,
      "retention.ms"::binary,
      # config_value
      0,
      9,
      "172800000"::binary,
      # validate_only
      1
    >>

    {request, expected_binary}
  end

  # ============================================
  # Response Data (binary + expected struct)
  # ============================================

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
      # resources array length
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
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary
    >>

    expected_struct = %Kayrock.AlterConfigs.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      resources: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic1"
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
      # resources array length
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
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary
    >>

    expected_struct = %Kayrock.AlterConfigs.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      resources: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 2,
          resource_name: "topic1"
        }
      ]
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions
  # ============================================

  def captured_response_binary(version) do
    {binary, _struct} = response_data(version)
    binary
  end

  def error_response(version, opts \\ [])

  def error_response(0, opts) do
    error_code = Keyword.get(opts, :error_code, 45)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Configuration error"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # resources array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      # error_message
      error_message_len::16,
      error_message::binary,
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 45)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = "Configuration error"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # resources array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      # error_message
      error_message_len::16,
      error_message::binary,
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary
    >>
  end
end
