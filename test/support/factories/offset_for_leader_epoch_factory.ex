defmodule Kayrock.Test.Factories.OffsetForLeaderEpochFactory do
  @moduledoc """
  Factory for OffsetForLeaderEpoch API test data (V0-V3).

  API Key: 23
  Used to: Fetch offsets for leader epoch to support log truncation detection.

  Protocol structure:
  - V0-V1: Basic request with topics/partitions
  - V2: Adds replica_id
  - V3: Adds current_leader_epoch
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.OffsetForLeaderEpoch.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, leader_epoch: 5}]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      23,
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
      # leader_epoch
      0,
      0,
      0,
      5
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.OffsetForLeaderEpoch.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, leader_epoch: 10}]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      23,
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
      1,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch
      0,
      0,
      0,
      10
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    # V2 adds current_leader_epoch to partitions (no replica_id at this level)
    request = %Kayrock.OffsetForLeaderEpoch.V2.Request{
      correlation_id: 2,
      client_id: "test",
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, current_leader_epoch: 12, leader_epoch: 15}]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      23,
      # api_version
      0,
      2,
      # correlation_id
      0,
      0,
      0,
      2,
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
      # current_leader_epoch
      0,
      0,
      0,
      12,
      # leader_epoch
      0,
      0,
      0,
      15
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    # V3 adds replica_id at top level
    request = %Kayrock.OffsetForLeaderEpoch.V3.Request{
      correlation_id: 3,
      client_id: "test",
      replica_id: -1,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{partition: 0, current_leader_epoch: 20, leader_epoch: 18}]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      23,
      # api_version
      0,
      3,
      # correlation_id
      0,
      0,
      0,
      3,
      # client_id
      0,
      4,
      "test"::binary,
      # replica_id (-1 for consumer)
      255,
      255,
      255,
      255,
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
      # current_leader_epoch
      0,
      0,
      0,
      20,
      # leader_epoch
      0,
      0,
      0,
      18
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
      # error_code
      0,
      0,
      # partition
      0,
      0,
      0,
      0,
      # end_offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100
    >>

    expected_struct = %Kayrock.OffsetForLeaderEpoch.V0.Response{
      correlation_id: 0,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{error_code: 0, partition: 0, end_offset: 100}]
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
      # error_code
      0,
      0,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch
      0,
      0,
      0,
      10,
      # end_offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200
    >>

    expected_struct = %Kayrock.OffsetForLeaderEpoch.V1.Response{
      correlation_id: 1,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{error_code: 0, partition: 0, leader_epoch: 10, end_offset: 200}]
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
      # error_code
      0,
      0,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch
      0,
      0,
      0,
      15,
      # end_offset (300 = 0x12C)
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      44
    >>

    expected_struct = %Kayrock.OffsetForLeaderEpoch.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 0,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{error_code: 0, partition: 0, leader_epoch: 15, end_offset: 300}]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(3) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      3,
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
      1,
      # error_code
      0,
      0,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch
      0,
      0,
      0,
      20,
      # end_offset
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      244
    >>

    expected_struct = %Kayrock.OffsetForLeaderEpoch.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 50,
      topics: [
        %{
          topic: "topic1",
          partitions: [%{error_code: 0, partition: 0, leader_epoch: 20, end_offset: 500}]
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
      error_code::16-signed,
      # partition
      0,
      0,
      0,
      0,
      # end_offset (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 3)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
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
      error_code::16-signed,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch (-1 on error)
      255,
      255,
      255,
      255,
      # end_offset (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>
  end

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 3)
    correlation_id = Keyword.get(opts, :correlation_id, 2)

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
      error_code::16-signed,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch (-1 on error)
      255,
      255,
      255,
      255,
      # end_offset (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>
  end

  def error_response(3, opts) do
    error_code = Keyword.get(opts, :error_code, 3)
    correlation_id = Keyword.get(opts, :correlation_id, 3)

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
      error_code::16-signed,
      # partition
      0,
      0,
      0,
      0,
      # leader_epoch (-1 on error)
      255,
      255,
      255,
      255,
      # end_offset (-1 on error)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>
  end
end
