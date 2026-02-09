defmodule Kayrock.Test.Factories.RenewDelegationTokenFactory do
  @moduledoc """
  Factory for RenewDelegationToken API test data (V0-V1).

  API Key: 39
  Used to: Renew a delegation token to extend its validity.

  Protocol structure:
  - V0-V1: Same schema - hmac (bytes) + renew_period_ms (int64)
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

    request = %Kayrock.RenewDelegationToken.V0.Request{
      correlation_id: 0,
      client_id: "test",
      hmac: hmac_data,
      renew_period_ms: 86_400_000
    }

    expected_binary = <<
      # api_key
      0,
      39,
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
      # hmac (16 bytes)
      0,
      0,
      0,
      16,
      hmac_data::binary,
      # renew_period_ms (86400000 = 24 hours)
      0,
      0,
      0,
      0,
      5,
      38,
      92,
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>

    request = %Kayrock.RenewDelegationToken.V1.Request{
      correlation_id: 1,
      client_id: "test",
      hmac: hmac_data,
      renew_period_ms: 86_400_000
    }

    expected_binary = <<
      # api_key
      0,
      39,
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
      # hmac (16 bytes)
      0,
      0,
      0,
      16,
      hmac_data::binary,
      # renew_period_ms
      0,
      0,
      0,
      0,
      5,
      38,
      92,
      0
    >>

    {request, expected_binary}
  end

  # ============================================
  # Response Data (binary + expected struct)
  # ============================================

  def response_data(0) do
    expiry_ts = 1_705_086_400_000

    binary = <<
      # correlation_id
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # expiry_timestamp_ms
      expiry_ts::64-signed,
      # throttle_time_ms
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.RenewDelegationToken.V0.Response{
      correlation_id: 0,
      error_code: 0,
      expiry_timestamp_ms: expiry_ts,
      throttle_time_ms: 0
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    expiry_ts = 1_705_086_400_000

    binary = <<
      # correlation_id
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # expiry_timestamp_ms
      expiry_ts::64-signed,
      # throttle_time_ms
      0,
      0,
      0,
      50
    >>

    expected_struct = %Kayrock.RenewDelegationToken.V1.Response{
      correlation_id: 1,
      error_code: 0,
      expiry_timestamp_ms: expiry_ts,
      throttle_time_ms: 50
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
    error_code = Keyword.get(opts, :error_code, 61)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      error_code::16-signed,
      # expiry_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 61)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      error_code::16-signed,
      # expiry_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0
    >>
  end
end
