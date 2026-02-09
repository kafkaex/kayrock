defmodule Kayrock.Test.Factories.InitProducerIdFactory do
  @moduledoc """
  Factory for InitProducerId API test data (V0-V2).

  API Key: 22
  Used to: Initialize a producer ID for idempotent/transactional producing.

  Protocol structure:
  - V0-V1: Basic init with transactional_id and timeout
  - V2: Flexible/compact format with tagged_fields
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.InitProducerId.V0.Request{
      correlation_id: 0,
      client_id: "test",
      transactional_id: "txn-1",
      transaction_timeout_ms: 60_000
    }

    expected_binary = <<
      # api_key
      0,
      22,
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
      # transactional_id (nullable string)
      0,
      5,
      "txn-1"::binary,
      # transaction_timeout_ms
      0,
      0,
      234,
      96
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.InitProducerId.V1.Request{
      correlation_id: 1,
      client_id: "test",
      transactional_id: "txn-1",
      transaction_timeout_ms: 60_000
    }

    expected_binary = <<
      # api_key
      0,
      22,
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
      # transactional_id (nullable string)
      0,
      5,
      "txn-1"::binary,
      # transaction_timeout_ms
      0,
      0,
      234,
      96
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.InitProducerId.V2.Request{
      correlation_id: 2,
      client_id: "test",
      transactional_id: "txn-1",
      transaction_timeout_ms: 60_000,
      tagged_fields: []
    }

    expected_binary = <<
      # api_key
      0,
      22,
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
      # header tagged_fields (empty)
      0,
      # transactional_id (compact nullable string, length + 1 = 6)
      6,
      "txn-1"::binary,
      # transaction_timeout_ms
      0,
      0,
      234,
      96,
      # tagged_fields (empty)
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
      # error_code
      0,
      0,
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
      0
    >>

    expected_struct = %Kayrock.InitProducerId.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      error_code: 0,
      producer_id: 1,
      producer_epoch: 0
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
      0,
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
      5
    >>

    expected_struct = %Kayrock.InitProducerId.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      error_code: 0,
      producer_id: 42,
      producer_epoch: 5
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
      # header tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # error_code
      0,
      0,
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
      # tagged_fields (empty)
      0
    >>

    expected_struct = %Kayrock.InitProducerId.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      error_code: 0,
      producer_id: 100,
      producer_epoch: 10,
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
    error_code = Keyword.get(opts, :error_code, 53)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      error_code::16-signed,
      # producer_id = -1 (invalid)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # producer_epoch = -1
      255,
      255
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 53)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      error_code::16-signed,
      # producer_id = -1
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # producer_epoch = -1
      255,
      255
    >>
  end

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 53)
    correlation_id = Keyword.get(opts, :correlation_id, 2)

    <<
      correlation_id::32,
      # header tagged_fields
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      error_code::16-signed,
      # producer_id = -1
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # producer_epoch = -1
      255,
      255,
      # tagged_fields
      0
    >>
  end

  def response_binary(version, opts) do
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    producer_id = Keyword.get(opts, :producer_id, 1)
    producer_epoch = Keyword.get(opts, :producer_epoch, 0)

    case version do
      v when v in [0, 1] ->
        <<
          version::32,
          throttle_time_ms::32,
          error_code::16-signed,
          producer_id::64-signed,
          producer_epoch::16-signed
        >>

      2 ->
        <<
          version::32,
          # header tagged_fields
          0,
          throttle_time_ms::32,
          error_code::16-signed,
          producer_id::64-signed,
          producer_epoch::16-signed,
          # tagged_fields
          0
        >>
    end
  end
end
