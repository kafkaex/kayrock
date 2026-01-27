defmodule Kayrock.Test.Factories.TxnOffsetCommitFactory do
  @moduledoc """
  Factory for TxnOffsetCommit API test data (V0-V2).

  API Key: 28
  Used to: Commit offsets within a transaction for a consumer group.

  Protocol structure:
  - V0-V2: Topics with partitions containing offset and metadata
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.TxnOffsetCommit.V0.Request{
      correlation_id: 0,
      client_id: "test",
      transactional_id: "txn-1",
      group_id: "group1",
      producer_id: 1,
      producer_epoch: 0,
      topics: [
        %{
          name: "topic1",
          partitions: [
            %{partition_index: 0, committed_offset: 100, committed_metadata: "meta"}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      28,
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
      # transactional_id
      0,
      5,
      "txn-1"::binary,
      # group_id
      0,
      6,
      "group1"::binary,
      # producer_id
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      # producer_epoch
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
      # committed_offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # committed_metadata
      0,
      4,
      "meta"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.TxnOffsetCommit.V1.Request{
      correlation_id: 1,
      client_id: "test",
      transactional_id: "txn-1",
      group_id: "group1",
      producer_id: 42,
      producer_epoch: 5,
      topics: [
        %{
          name: "topic1",
          partitions: [
            %{partition_index: 0, committed_offset: 200, committed_metadata: nil}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      28,
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
      # transactional_id
      0,
      5,
      "txn-1"::binary,
      # group_id
      0,
      6,
      "group1"::binary,
      # producer_id
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      42,
      # producer_epoch
      0,
      5,
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
      # committed_offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200,
      # committed_metadata (null)
      255,
      255
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    # V2 adds committed_leader_epoch to partitions
    request = %Kayrock.TxnOffsetCommit.V2.Request{
      correlation_id: 2,
      client_id: "test",
      transactional_id: "txn-1",
      group_id: "group1",
      producer_id: 100,
      producer_epoch: 10,
      topics: [
        %{
          name: "topic1",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 500,
              committed_leader_epoch: 5,
              committed_metadata: "v2"
            }
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      28,
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
      # transactional_id
      0,
      5,
      "txn-1"::binary,
      # group_id
      0,
      6,
      "group1"::binary,
      # producer_id
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # producer_epoch
      0,
      10,
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
      # committed_offset (500)
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      244,
      # committed_leader_epoch
      0,
      0,
      0,
      5,
      # committed_metadata
      0,
      2,
      "v2"::binary
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
      # partition_index
      0,
      0,
      0,
      0,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.TxnOffsetCommit.V0.Response{
      correlation_id: 0,
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

    expected_struct = %Kayrock.TxnOffsetCommit.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      topics: [
        %{
          name: "topic1",
          partitions: [%{partition_index: 0, error_code: 0}]
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
      # error_code
      0,
      0,
      # partition 1
      0,
      0,
      0,
      1,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.TxnOffsetCommit.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      topics: [
        %{
          name: "topic1",
          partitions: [
            %{partition_index: 0, error_code: 0},
            %{partition_index: 1, error_code: 0}
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
    error_code = Keyword.get(opts, :error_code, 51)
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
      # partition_index
      0,
      0,
      0,
      0,
      error_code::16-signed
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 51)
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
      # partition_index
      0,
      0,
      0,
      0,
      error_code::16-signed
    >>
  end

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 51)
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
      # partition_index
      0,
      0,
      0,
      0,
      error_code::16-signed
    >>
  end
end
