defmodule Kayrock.Test.Factories.OffsetDeleteFactory do
  @moduledoc """
  Factory for OffsetDelete API test data (V0).

  API Key: 47
  Used to: Delete committed offsets for a consumer group.

  Protocol structure:
  - V0: group_id with topics containing partition indices
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.OffsetDelete.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "group1",
      topics: [
        %{
          name: "topic1",
          partitions: [%{partition_index: 0}]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      47,
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
      # group_id
      0,
      6,
      "group1"::binary,
      # topics array length
      0,
      0,
      0,
      1,
      # topic name
      0,
      6,
      "topic1"::binary,
      # partitions array length
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
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
      # error_code (top-level)
      0,
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # topics array length
      0,
      0,
      0,
      1,
      # topic name
      0,
      6,
      "topic1"::binary,
      # partitions array length
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
      0,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetDelete.V0.Response{
      correlation_id: 0,
      error_code: 0,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic1",
          partitions: [%{partition_index: 0, error_code: 0}]
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
    error_code = Keyword.get(opts, :error_code, 69)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      # top-level error_code
      0,
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # topics array length
      0,
      0,
      0,
      1,
      # topic name
      0,
      6,
      "topic1"::binary,
      # partitions array length
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
      0,
      error_code::16-signed
    >>
  end
end
