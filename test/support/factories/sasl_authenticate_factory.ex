defmodule Kayrock.Test.Factories.SaslAuthenticateFactory do
  @moduledoc """
  Factory for SaslAuthenticate API test data (V0-V1).

  API Key: 36
  Used to: Perform SASL authentication after handshake.

  Protocol structure:
  - V0: Basic auth with bytes exchange
  - V1: Adds session_lifetime_ms in response
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    auth_bytes = <<0, "user", 0, "password">>

    request = %Kayrock.SaslAuthenticate.V0.Request{
      correlation_id: 0,
      client_id: "test",
      auth_bytes: auth_bytes
    }

    expected_binary = <<
      # api_key
      0,
      36,
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
      # auth_bytes length
      0,
      0,
      0,
      14,
      # auth_bytes
      auth_bytes::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    auth_bytes = <<0, "user", 0, "password">>

    request = %Kayrock.SaslAuthenticate.V1.Request{
      correlation_id: 1,
      client_id: "test",
      auth_bytes: auth_bytes
    }

    expected_binary = <<
      # api_key
      0,
      36,
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
      # auth_bytes length
      0,
      0,
      0,
      14,
      # auth_bytes
      auth_bytes::binary
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
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255,
      # auth_bytes length
      0,
      0,
      0,
      8,
      # auth_bytes
      "response"::binary
    >>

    expected_struct = %Kayrock.SaslAuthenticate.V0.Response{
      correlation_id: 0,
      error_code: 0,
      error_message: nil,
      auth_bytes: "response"
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
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255,
      # auth_bytes length
      0,
      0,
      0,
      8,
      # auth_bytes
      "response"::binary,
      # session_lifetime_ms
      0,
      0,
      0,
      0,
      0,
      54,
      238,
      128
    >>

    expected_struct = %Kayrock.SaslAuthenticate.V1.Response{
      correlation_id: 1,
      error_code: 0,
      error_message: nil,
      auth_bytes: "response",
      session_lifetime_ms: 3_600_000
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
    error_code = Keyword.get(opts, :error_code, 34)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = Keyword.get(opts, :error_message, "Authentication failed")

    <<
      correlation_id::32,
      error_code::16-signed,
      byte_size(error_message)::16,
      error_message::binary,
      # empty auth_bytes
      0,
      0,
      0,
      0
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 34)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = Keyword.get(opts, :error_message, "Authentication failed")

    <<
      correlation_id::32,
      error_code::16-signed,
      byte_size(error_message)::16,
      error_message::binary,
      # empty auth_bytes
      0,
      0,
      0,
      0,
      # session_lifetime_ms = 0
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0
    >>
  end
end
