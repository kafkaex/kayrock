defmodule Kayrock.Test.Factories.DeleteRecordsFactory do
  @moduledoc """
  Factory for DeleteRecords API test data (V0-V1).

  API Key: 21
  Used to: Delete records from a topic partition up to a specified offset.

  Protocol structure:
  - V0-V1: Same schema - topics with partitions and offsets
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.DeleteRecords.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, offset: 100}]
        }
      ],
      timeout: 30000
    }

    expected_binary = <<
      # api_key
      0,
      21,
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
      # partition
      0,
      0,
      0,
      0,
      # offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # timeout
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DeleteRecords.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topics: [
        %{
          topic: "topic1",
          partitions: [
            %{partition: 0, offset: 200},
            %{partition: 1, offset: 150}
          ]
        }
      ],
      timeout: 60000
    }

    expected_binary = <<
      # api_key
      0,
      21,
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
      2,
      # partition 0
      0,
      0,
      0,
      0,
      # offset 200
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200,
      # partition 1
      0,
      0,
      0,
      1,
      # offset 150
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      150,
      # timeout (60000)
      0,
      0,
      234,
      96
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
      # partition
      0,
      0,
      0,
      0,
      # low_watermark
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteRecords.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, low_watermark: 100, error_code: 0}]
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
      2,
      # partition 0
      0,
      0,
      0,
      0,
      # low_watermark
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200,
      # error_code
      0,
      0,
      # partition 1
      0,
      0,
      0,
      1,
      # low_watermark
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      150,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteRecords.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      topics: [
        %{
          topic: "topic1",
          partitions: [
            %{partition: 0, low_watermark: 200, error_code: 0},
            %{partition: 1, low_watermark: 150, error_code: 0}
          ]
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
    error_code = Keyword.get(opts, :error_code, 3)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
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
      # partition
      0,
      0,
      0,
      0,
      # low_watermark (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      error_code::16-signed
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 3)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
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
      # partition
      0,
      0,
      0,
      0,
      # low_watermark (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      error_code::16-signed
    >>
  end
end
