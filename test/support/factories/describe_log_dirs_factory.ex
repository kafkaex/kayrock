defmodule Kayrock.Test.Factories.DescribeLogDirsFactory do
  @moduledoc """
  Factory for DescribeLogDirs API test data (V0-V1).

  API Key: 35
  Used to: Describe the log directories on brokers.

  Protocol structure:
  - V0-V1: Same schema - topics with partitions, response has log_dirs with size/offset_lag
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.DescribeLogDirs.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topics: [
        %{topic: "topic1", partitions: [0, 1]}
      ]
    }

    expected_binary = <<
      # api_key
      0,
      35,
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
      # topic
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
      # partition 1
      0,
      0,
      0,
      1
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DescribeLogDirs.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topics: [
        %{topic: "topic1", partitions: [0]}
      ]
    }

    expected_binary = <<
      # api_key
      0,
      35,
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
      # topic
      0,
      6,
      "topic1"::binary,
      # partitions array length
      0,
      0,
      0,
      1,
      # partition 0
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
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # log_dirs array length
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # log_dir
      0,
      12,
      "/var/log/dir"::binary,
      # topics array length
      0,
      0,
      0,
      1,
      # topic
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
      # size (1024)
      0,
      0,
      0,
      0,
      0,
      0,
      4,
      0,
      # offset_lag (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # is_future (false)
      0
    >>

    expected_struct = %Kayrock.DescribeLogDirs.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      log_dirs: [
        %{
          error_code: 0,
          log_dir: "/var/log/dir",
          topics: [
            %{
              topic: "topic1",
              partitions: [
                %{partition: 0, size: 1024, offset_lag: 0, is_future: 0}
              ]
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
      # log_dirs array length
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # log_dir
      0,
      12,
      "/var/log/dir"::binary,
      # topics array length
      0,
      0,
      0,
      1,
      # topic
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
      # size (2048)
      0,
      0,
      0,
      0,
      0,
      0,
      8,
      0,
      # offset_lag (10)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      10,
      # is_future (false)
      0
    >>

    expected_struct = %Kayrock.DescribeLogDirs.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      log_dirs: [
        %{
          error_code: 0,
          log_dir: "/var/log/dir",
          topics: [
            %{
              topic: "topic1",
              partitions: [
                %{partition: 0, size: 2048, offset_lag: 10, is_future: 0}
              ]
            }
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
    error_code = Keyword.get(opts, :error_code, 58)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # log_dirs array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      # log_dir
      0,
      12,
      "/var/log/dir"::binary,
      # topics array length (empty)
      0,
      0,
      0,
      0
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 58)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # log_dirs array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      # log_dir
      0,
      12,
      "/var/log/dir"::binary,
      # topics array length (empty)
      0,
      0,
      0,
      0
    >>
  end
end
