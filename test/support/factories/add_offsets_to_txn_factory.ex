defmodule Kayrock.Test.Factories.AddOffsetsToTxnFactory do
  @moduledoc """
  Factory for AddOffsetsToTxn API test data (V0-V1).

  API Key: 25
  Used to: Add offsets to a transaction for a consumer group.

  Protocol structure:
  - V0-V1: Same schema - throttle_time_ms and error_code in response
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.AddOffsetsToTxn.V0.Request{
      correlation_id: 0,
      client_id: "test",
      transactional_id: "txn-1",
      producer_id: 1,
      producer_epoch: 0,
      group_id: "group1"
    }

    expected_binary = <<
      # api_key
      0,
      25,
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
      # group_id
      0,
      6,
      "group1"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.AddOffsetsToTxn.V1.Request{
      correlation_id: 1,
      client_id: "test",
      transactional_id: "txn-1",
      producer_id: 42,
      producer_epoch: 5,
      group_id: "group1"
    }

    expected_binary = <<
      # api_key
      0,
      25,
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
      # group_id
      0,
      6,
      "group1"::binary
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.AddOffsetsToTxn.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      error_code: 0
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.AddOffsetsToTxn.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      error_code: 0
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
      error_code::16-signed
    >>
  end
end
