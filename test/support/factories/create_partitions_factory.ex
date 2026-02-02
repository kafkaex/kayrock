defmodule Kayrock.Test.Factories.CreatePartitionsFactory do
  @moduledoc """
  Factory for CreatePartitions API test data (V0-V1).

  API Key: 37
  Used to: Create additional partitions for existing topics.

  Protocol structure:
  - V0-V1: Same schema - topic_partitions with new_partitions count
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.CreatePartitions.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topic_partitions: [
        %{
          topic: "topic1",
          new_partitions: %{count: 6, assignment: []}
        }
      ],
      timeout: 30_000,
      validate_only: false
    }

    expected_binary = <<
      # api_key
      0,
      37,
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
      # topic_partitions array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # new_partitions.count
      0,
      0,
      0,
      6,
      # new_partitions.assignment array length (empty)
      0,
      0,
      0,
      0,
      # timeout
      0,
      0,
      117,
      48,
      # validate_only
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.CreatePartitions.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topic_partitions: [
        %{
          topic: "topic1",
          new_partitions: %{count: 8, assignment: []}
        }
      ],
      timeout: 60_000,
      validate_only: true
    }

    expected_binary = <<
      # api_key
      0,
      37,
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
      # topic_partitions array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # new_partitions.count
      0,
      0,
      0,
      8,
      # new_partitions.assignment array length (empty)
      0,
      0,
      0,
      0,
      # timeout (60_000)
      0,
      0,
      234,
      96,
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
      # topic_errors array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.CreatePartitions.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      topic_errors: [
        %{topic: "topic1", error_code: 0, error_message: nil}
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
      # topic_errors array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.CreatePartitions.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      topic_errors: [
        %{topic: "topic1", error_code: 0, error_message: nil}
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
    error_code = Keyword.get(opts, :error_code, 37)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Invalid partition count"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # topic_errors array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 37)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = "Invalid partition count"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # topic_errors array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary
    >>
  end
end
