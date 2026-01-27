defmodule Kayrock.Test.Factories.CreateDelegationTokenFactory do
  @moduledoc """
  Factory for CreateDelegationToken API test data (V0-V2).

  API Key: 38
  Used to: Create a delegation token for authentication.

  Protocol structure:
  - V0-V1: Regular format - renewers array + max_lifetime_ms
  - V2: Flexible/compact format - same structure with tagged_fields
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.CreateDelegationToken.V0.Request{
      correlation_id: 0,
      client_id: "test",
      renewers: [
        %{principal_type: "User", principal_name: "admin"}
      ],
      max_lifetime_ms: 86_400_000
    }

    expected_binary = <<
      # api_key
      0,
      38,
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
      # renewers array length
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
      # max_lifetime_ms (86400000 = 24 hours)
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
    request = %Kayrock.CreateDelegationToken.V1.Request{
      correlation_id: 1,
      client_id: "test",
      renewers: [
        %{principal_type: "User", principal_name: "admin"}
      ],
      max_lifetime_ms: 86_400_000
    }

    expected_binary = <<
      # api_key
      0,
      38,
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
      # renewers array length
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
      # max_lifetime_ms
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

  def request_data(2) do
    request = %Kayrock.CreateDelegationToken.V2.Request{
      correlation_id: 2,
      client_id: "test",
      renewers: [
        %{principal_type: "User", principal_name: "admin", tagged_fields: []}
      ],
      max_lifetime_ms: 86_400_000,
      tagged_fields: []
    }

    # Captured from actual serialization
    expected_binary = <<
      0,
      38,
      0,
      2,
      0,
      0,
      0,
      2,
      0,
      4,
      116,
      101,
      115,
      116,
      0,
      2,
      5,
      85,
      115,
      101,
      114,
      6,
      97,
      100,
      109,
      105,
      110,
      0,
      0,
      0,
      0,
      0,
      5,
      38,
      92,
      0,
      0
    >>

    {request, expected_binary}
  end

  # ============================================
  # Response Data (binary + expected struct)
  # ============================================

  def response_data(0) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16>>
    # Timestamps: 1705000000000, 1705086400000, 1705200000000
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
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary,
      # issue_timestamp_ms
      issue_ts::64-signed,
      # expiry_timestamp_ms
      expiry_ts::64-signed,
      # max_timestamp_ms
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
      # throttle_time_ms
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.CreateDelegationToken.V0.Response{
      correlation_id: 0,
      error_code: 0,
      principal_type: "User",
      principal_name: "admin",
      issue_timestamp_ms: issue_ts,
      expiry_timestamp_ms: expiry_ts,
      max_timestamp_ms: max_ts,
      token_id: "token12345",
      hmac: hmac_data,
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
      # principal_type
      0,
      4,
      "User"::binary,
      # principal_name
      0,
      5,
      "admin"::binary,
      # issue_timestamp_ms
      issue_ts::64-signed,
      # expiry_timestamp_ms
      expiry_ts::64-signed,
      # max_timestamp_ms
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
      # throttle_time_ms
      0,
      0,
      0,
      50
    >>

    expected_struct = %Kayrock.CreateDelegationToken.V1.Response{
      correlation_id: 1,
      error_code: 0,
      principal_type: "User",
      principal_name: "admin",
      issue_timestamp_ms: issue_ts,
      expiry_timestamp_ms: expiry_ts,
      max_timestamp_ms: max_ts,
      token_id: "token12345",
      hmac: hmac_data,
      throttle_time_ms: 50
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    hmac_data = <<1, 2, 3, 4, 5, 6, 7, 8>>
    issue_ts = 1_705_000_000_000
    expiry_ts = 1_705_086_400_000
    max_ts = 1_705_200_000_000

    binary = <<
      # correlation_id
      0,
      0,
      0,
      2,
      # HEADER tagged_fields (empty)
      0,
      # error_code
      0,
      0,
      # principal_type: compact_string length+1 = 5
      5,
      "User"::binary,
      # principal_name: compact_string length+1 = 6
      6,
      "admin"::binary,
      # issue_timestamp_ms
      issue_ts::64-signed,
      # expiry_timestamp_ms
      expiry_ts::64-signed,
      # max_timestamp_ms
      max_ts::64-signed,
      # token_id: compact_string length+1 = 11
      11,
      "token12345"::binary,
      # hmac: compact_bytes length+1 = 9 (8 bytes)
      9,
      hmac_data::binary,
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # tagged_fields (empty)
      0
    >>

    expected_struct = %Kayrock.CreateDelegationToken.V2.Response{
      correlation_id: 2,
      error_code: 0,
      principal_type: "User",
      principal_name: "admin",
      issue_timestamp_ms: issue_ts,
      expiry_timestamp_ms: expiry_ts,
      max_timestamp_ms: max_ts,
      token_id: "token12345",
      hmac: hmac_data,
      throttle_time_ms: 100,
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
    error_code = Keyword.get(opts, :error_code, 60)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      error_code::16-signed,
      # principal_type (empty)
      0,
      0,
      # principal_name (empty)
      0,
      0,
      # issue_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # expiry_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # max_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # token_id (empty)
      0,
      0,
      # hmac (empty)
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
    error_code = Keyword.get(opts, :error_code, 60)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      error_code::16-signed,
      # principal_type (empty)
      0,
      0,
      # principal_name (empty)
      0,
      0,
      # issue_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # expiry_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # max_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # token_id (empty)
      0,
      0,
      # hmac (empty)
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

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 60)
    correlation_id = Keyword.get(opts, :correlation_id, 2)

    <<
      correlation_id::32,
      # HEADER tagged_fields (empty)
      0,
      error_code::16-signed,
      # principal_type: compact_string (empty = 1)
      1,
      # principal_name: compact_string (empty = 1)
      1,
      # issue_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # expiry_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # max_timestamp_ms
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # token_id: compact_string (empty = 1)
      1,
      # hmac: compact_bytes (empty = 1)
      1,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # tagged_fields
      0
    >>
  end
end
