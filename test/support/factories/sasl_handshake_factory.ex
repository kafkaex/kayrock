defmodule Kayrock.Test.Factories.SaslHandshakeFactory do
  @moduledoc """
  Factory for SaslHandshake API test data (V0-V1).

  API Key: 17
  Used to: Initiate SASL authentication handshake with the broker.

  Protocol structure:
  - V0-V1: Request mechanism, response with available mechanisms
  - V0 vs V1: Minor version bump for compatibility
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.SaslHandshake.V0.Request{
      correlation_id: 0,
      client_id: "test",
      mechanism: "PLAIN"
    }

    expected_binary = <<
      # api_key
      0,
      17,
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
      # mechanism
      0,
      5,
      "PLAIN"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.SaslHandshake.V1.Request{
      correlation_id: 1,
      client_id: "test",
      mechanism: "SCRAM-SHA-256"
    }

    expected_binary = <<
      # api_key
      0,
      17,
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
      # mechanism
      0,
      13,
      "SCRAM-SHA-256"::binary
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
      # mechanisms array length
      0,
      0,
      0,
      2,
      # mechanism 1
      0,
      5,
      "PLAIN"::binary,
      # mechanism 2
      0,
      13,
      "SCRAM-SHA-256"::binary
    >>

    # Note: deserialize_array reverses the list
    expected_struct = %Kayrock.SaslHandshake.V0.Response{
      correlation_id: 0,
      error_code: 0,
      mechanisms: ["PLAIN", "SCRAM-SHA-256"]
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
      # mechanisms array length
      0,
      0,
      0,
      3,
      # mechanism 1
      0,
      5,
      "PLAIN"::binary,
      # mechanism 2
      0,
      13,
      "SCRAM-SHA-256"::binary,
      # mechanism 3
      0,
      13,
      "SCRAM-SHA-512"::binary
    >>

    # Note: deserialize_array reverses the list
    expected_struct = %Kayrock.SaslHandshake.V1.Response{
      correlation_id: 1,
      error_code: 0,
      mechanisms: ["PLAIN", "SCRAM-SHA-256", "SCRAM-SHA-512"]
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
    error_code = Keyword.get(opts, :error_code, 33)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      error_code::16-signed,
      # empty mechanisms array
      0,
      0,
      0,
      0
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 33)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      error_code::16-signed,
      # empty mechanisms array
      0,
      0,
      0,
      0
    >>
  end

  def response_with_mechanisms(version, mechanisms) when is_list(mechanisms) do
    mechanism_binaries =
      Enum.map(mechanisms, fn mech ->
        <<byte_size(mech)::16, mech::binary>>
      end)

    base =
      case version do
        0 -> <<0, 0, 0, 0>>
        1 -> <<0, 0, 0, 1>>
      end

    IO.iodata_to_binary([
      base,
      # error_code = 0
      <<0, 0>>,
      # array length
      <<length(mechanisms)::32>>,
      mechanism_binaries
    ])
  end
end
