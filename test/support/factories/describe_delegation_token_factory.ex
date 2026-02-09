defmodule Kayrock.Test.Factories.DescribeDelegationTokenFactory do
  @moduledoc """
  Factory for DescribeDelegationToken API test data (V0-V1).

  API Key: 41
  Used to: Describe existing delegation tokens.

  Protocol structure:
  - V0-V1: Same schema - owners array for request, tokens array for response
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.DescribeDelegationToken.V0.Request{
      correlation_id: 0,
      client_id: "test",
      owners: [
        %{principal_type: "User", principal_name: "admin"}
      ]
    }

    expected_binary = <<
      # api_key
      0,
      41,
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
      # owners array length
      0,
      0,
      0,
      1,
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DescribeDelegationToken.V1.Request{
      correlation_id: 1,
      client_id: "test",
      owners: [
        %{principal_type: "User", principal_name: "admin"}
      ]
    }

    expected_binary = <<
      # api_key
      0,
      41,
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
      # owners array length
      0,
      0,
      0,
      1,
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary
    >>

    {request, expected_binary}
  end

  # ============================================
  # Response Data (binary + expected struct)
  # ============================================

  def response_data(0) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
    issue_ts = 1_705_000_000_000
    expiry_ts = 1_705_086_400_000
    max_ts = 1_705_200_000_000

    binary = <<
      # correlation_id
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # tokens array length
      0,
      0,
      0,
      1,
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary,
      # issue_timestamp
      issue_ts::64-signed,
      # expiry_timestamp
      expiry_ts::64-signed,
      # max_timestamp
      max_ts::64-signed,
      # token_id
      0,
      10,
      "token12345"::binary,
      # hmac (16 bytes)
      0,
      0,
      0,
      16,
      hmac_data::binary,
      # renewers array length
      0,
      0,
      0,
      1,
      # renewer principal_type
      0,
      4,
      "User"::binary,
      # renewer principal_name
      0,
      5,
      "admin"::binary,
      # throttle_time_ms
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeDelegationToken.V0.Response{
      correlation_id: 0,
      error_code: 0,
      tokens: [
        %{
          principal_type: "User",
          principal_name: "admin",
          issue_timestamp: issue_ts,
          expiry_timestamp: expiry_ts,
          max_timestamp: max_ts,
          token_id: "token12345",
          hmac: hmac_data,
          renewers: [
            %{principal_type: "User", principal_name: "admin"}
          ]
        }
      ],
      throttle_time_ms: 0
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
    issue_ts = 1_705_000_000_000
    expiry_ts = 1_705_086_400_000
    max_ts = 1_705_200_000_000

    binary = <<
      # correlation_id
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # tokens array length
      0,
      0,
      0,
      1,
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary,
      # issue_timestamp
      issue_ts::64-signed,
      # expiry_timestamp
      expiry_ts::64-signed,
      # max_timestamp
      max_ts::64-signed,
      # token_id
      0,
      10,
      "token12345"::binary,
      # hmac (16 bytes)
      0,
      0,
      0,
      16,
      hmac_data::binary,
      # renewers array length
      0,
      0,
      0,
      1,
      # renewer principal_type
      0,
      4,
      "User"::binary,
      # renewer principal_name
      0,
      5,
      "admin"::binary,
      # throttle_time_ms
      0,
      0,
      0,
      50
    >>

    expected_struct = %Kayrock.DescribeDelegationToken.V1.Response{
      correlation_id: 1,
      error_code: 0,
      tokens: [
        %{
          principal_type: "User",
          principal_name: "admin",
          issue_timestamp: issue_ts,
          expiry_timestamp: expiry_ts,
          max_timestamp: max_ts,
          token_id: "token12345",
          hmac: hmac_data,
          renewers: [
            %{principal_type: "User", principal_name: "admin"}
          ]
        }
      ],
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
    error_code = Keyword.get(opts, :error_code, 59)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      error_code::16-signed,
      # tokens array (empty)
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
    error_code = Keyword.get(opts, :error_code, 59)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      error_code::16-signed,
      # tokens array (empty)
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
