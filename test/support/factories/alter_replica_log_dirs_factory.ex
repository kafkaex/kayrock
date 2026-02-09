defmodule Kayrock.Test.Factories.AlterReplicaLogDirsFactory do
  @moduledoc """
  Factory for AlterReplicaLogDirs API test data (V0-V1).

  API Key: 34
  Used to: Move replicas to different log directories.

  Protocol structure:
  - V0-V1: Same schema - log_dirs array with topics and partitions
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.AlterReplicaLogDirs.V0.Request{
      correlation_id: 0,
      client_id: "test",
      log_dirs: [
        %{
          log_dir: "/var/kafka/data1",
          topics: [
            %{topic: "topic1", partitions: [0, 1]}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      34,
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
      # log_dirs array length
      0,
      0,
      0,
      1,
      # log_dir
      0,
      16,
      "/var/kafka/data1"::binary,
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
    request = %Kayrock.AlterReplicaLogDirs.V1.Request{
      correlation_id: 1,
      client_id: "test",
      log_dirs: [
        %{
          log_dir: "/var/kafka/data2",
          topics: [
            %{topic: "topic1", partitions: [0]}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      34,
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
      # log_dirs array length
      0,
      0,
      0,
      1,
      # log_dir
      0,
      16,
      "/var/kafka/data2"::binary,
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.AlterReplicaLogDirs.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      topics: [
        %{
          topic: "topic1",
          partitions: [
            %{partition: 0, error_code: 0}
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.AlterReplicaLogDirs.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      topics: [
        %{
          topic: "topic1",
          partitions: [
            %{partition: 0, error_code: 0}
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
      error_code::16-signed
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
      error_code::16-signed
    >>
  end
end
