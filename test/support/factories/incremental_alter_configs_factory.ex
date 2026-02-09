defmodule Kayrock.Test.Factories.IncrementalAlterConfigsFactory do
  @moduledoc """
  Factory for IncrementalAlterConfigs API test data (V0-V1).

  API Key: 44
  Used to: Incrementally alter broker/topic configurations.

  Protocol structure:
  - V0-V1: Same schema - resources array with configs, validate_only flag
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.IncrementalAlterConfigs.V0.Request{
      correlation_id: 0,
      client_id: "test",
      resources: [
        %{
          resource_type: 2,
          resource_name: "topic1",
          configs: [
            %{name: "retention.ms", config_operation: 0, value: "604800000"}
          ]
        }
      ],
      validate_only: false
    }

    expected_binary = <<
      # api_key
      0,
      44,
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
      # configs array length
      0,
      0,
      0,
      1,
      # name
      0,
      12,
      "retention.ms"::binary,
      # config_operation (0 = SET)
      0,
      # value (nullable_string)
      0,
      9,
      "604800000"::binary,
      # validate_only (false = 0)
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    # V1 uses flexible/compact format
    request = %Kayrock.IncrementalAlterConfigs.V1.Request{
      correlation_id: 1,
      client_id: "test",
      resources: [
        %{
          resource_type: 4,
          resource_name: "0",
          configs: [
            %{name: "log.retention.ms", config_operation: 1, value: nil, tagged_fields: []}
          ],
          tagged_fields: []
        }
      ],
      validate_only: true,
      tagged_fields: []
    }

    # Captured from actual serialization
    expected_binary = <<
      0,
      44,
      0,
      1,
      0,
      0,
      0,
      1,
      0,
      4,
      116,
      101,
      115,
      116,
      0,
      2,
      4,
      2,
      48,
      2,
      17,
      108,
      111,
      103,
      46,
      114,
      101,
      116,
      101,
      110,
      116,
      105,
      111,
      110,
      46,
      109,
      115,
      1,
      0,
      0,
      0,
      1,
      0
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
      # responses array length
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

    expected_struct = %Kayrock.IncrementalAlterConfigs.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      responses: [
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
    # V1 uses flexible/compact format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      1,
      # HEADER tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      50,
      # responses compact_array length+1 = 2 (1 element)
      2,
      # error_code
      0,
      0,
      # error_message: compact_nullable_string (null = 0)
      0,
      # resource_type
      4,
      # resource_name: compact_string length+1 = 2
      2,
      "0"::binary,
      # tagged_fields for response entry
      0,
      # tagged_fields for root
      0
    >>

    expected_struct = %Kayrock.IncrementalAlterConfigs.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      responses: [
        %{
          error_code: 0,
          error_message: nil,
          resource_type: 4,
          resource_name: "0",
          tagged_fields: []
        }
      ],
      tagged_fields: []
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
    error_code = Keyword.get(opts, :error_code, 40)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Invalid config"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # responses array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
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
    error_code = Keyword.get(opts, :error_code, 40)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = "Invalid config"
    # compact_string length+1 encoding
    error_message_len_plus_one = byte_size(error_message) + 1

    <<
      correlation_id::32,
      # HEADER tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # responses compact_array length+1 = 2
      2,
      error_code::16-signed,
      # error_message: compact_string
      error_message_len_plus_one::8,
      error_message::binary,
      # resource_type
      4,
      # resource_name: compact_string length+1 = 2
      2,
      "0"::binary,
      # tagged_fields for response entry
      0,
      # tagged_fields for root
      0
    >>
  end
end
