defmodule Kayrock.Test.Factories.ApiVersionsFactory do
  @moduledoc """
  Factory for generating ApiVersions test data.

  Provides request structs with expected binaries and response binaries with expected structs.
  All data captured from real Kafka 7.4.0 broker.
  """

  @doc """
  Returns request test data for the specified version.

  Returns a tuple of `{struct, expected_binary}` where:
  - `struct` is the Request struct to serialize
  - `expected_binary` is the expected serialized output (captured from Kafka 7.4.0)

  ## Examples

      iex> {request, expected_binary} = request_data(0)
      iex> serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      iex> assert serialized == expected_binary
  """
  def request_data(version)

  def request_data(0) do
    request = %Kayrock.ApiVersions.V0.Request{
      correlation_id: 0,
      client_id: "kayrock-capture"
    }

    expected_binary = <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (0)
      0,
      0,
      # correlation_id (0)
      0,
      0,
      0,
      0,
      # client_id length (15)
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.ApiVersions.V1.Request{
      correlation_id: 1,
      client_id: "kayrock-capture"
    }

    expected_binary = <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (1)
      0,
      1,
      # correlation_id (1)
      0,
      0,
      0,
      1,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.ApiVersions.V2.Request{
      correlation_id: 2,
      client_id: "kayrock-capture"
    }

    expected_binary = <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (2)
      0,
      2,
      # correlation_id (2)
      0,
      0,
      0,
      2,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.ApiVersions.V3.Request{
      correlation_id: 3,
      client_id: "kayrock-capture",
      client_software_name: "kayrock",
      client_software_version: "1.0.0"
    }

    expected_binary = <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (3)
      0,
      3,
      # correlation_id (3)
      0,
      0,
      0,
      3,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary,
      # flexible header marker
      0,
      # client_software_name (compact string: length+1)
      8,
      # client_software_name
      "kayrock"::binary,
      # client_software_version (compact string: length+1)
      6,
      # client_software_version
      "1.0.0"::binary,
      # tagged_fields count
      0
    >>

    {request, expected_binary}
  end

  @doc """
  Returns response test data for the specified version.

  Returns a tuple of `{binary, expected_struct}` where:
  - `binary` is the response binary to deserialize (captured from Kafka 7.4.0)
  - `expected_struct` is the expected deserialized struct

  ## Examples

      iex> {response_binary, expected_struct} = response_data(0)
      iex> {actual, <<>>} = Kayrock.ApiVersions.V0.Response.deserialize(response_binary)
      iex> assert actual == expected_struct
  """
  def response_data(version)

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
      # api_keys array length (2)
      0,
      0,
      0,
      2,
      # Produce: api_key=0, min=0, max=9
      0,
      0,
      0,
      0,
      0,
      9,
      # Fetch: api_key=1, min=0, max=13
      0,
      1,
      0,
      0,
      0,
      13
    >>

    expected_struct = %Kayrock.ApiVersions.V0.Response{
      correlation_id: 0,
      error_code: 0,
      api_keys: [
        %{api_key: 0, min_version: 0, max_version: 9},
        %{api_key: 1, min_version: 0, max_version: 13}
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
      # error_code
      0,
      0,
      # api_keys count
      0,
      0,
      0,
      2,
      # Produce V0-9
      0,
      0,
      0,
      0,
      0,
      9,
      # Fetch V0-13
      0,
      1,
      0,
      0,
      0,
      13,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.ApiVersions.V1.Response{
      correlation_id: 1,
      error_code: 0,
      throttle_time_ms: 0,
      api_keys: [
        %{api_key: 0, min_version: 0, max_version: 9},
        %{api_key: 1, min_version: 0, max_version: 13}
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
      # error_code
      0,
      0,
      # empty api_keys
      0,
      0,
      0,
      0,
      # throttle_time_ms (50)
      0,
      0,
      0,
      50
    >>

    expected_struct = %Kayrock.ApiVersions.V2.Response{
      correlation_id: 2,
      error_code: 0,
      throttle_time_ms: 50,
      api_keys: []
    }

    {binary, expected_struct}
  end

  def response_data(3) do
    # V3 response not yet supported by Kafka 7.4.0
    raise "V3 response format not yet captured from broker - broker returns UNSUPPORTED_VERSION"
  end

  # ============================================
  # Legacy API (for backward compatibility)
  # ============================================

  @doc """
  Returns just the captured request binary for the specified version.

  For new code, prefer `request_data/1` which returns `{struct, binary}`.
  """
  def captured_request_binary(version) do
    {_struct, binary} = request_data(version)
    binary
  end

  @doc """
  Returns just the captured response binary for the specified version.

  For new code, prefer `response_data/1` which returns `{binary, struct}`.
  """
  def captured_response_binary(version) do
    {binary, _struct} = response_data(version)
    binary
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary with specified options.

  ## Options
  - `:correlation_id` - Default: 0
  - `:error_code` - Default: 0
  - `:api_keys` - List of %{api_key, min_version, max_version} (default: 2 APIs)
  - `:throttle_time_ms` - V1+ only (default: 0)

  ## Examples

      iex> response_binary(0, api_keys: [%{api_key: 18, min_version: 0, max_version: 3}])
      # V0 response with single API

      iex> response_binary(1, throttle_time_ms: 100, correlation_id: 42)
      # V1 response with custom throttle and correlation_id
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    api_keys =
      Keyword.get(opts, :api_keys, [
        %{api_key: 0, min_version: 0, max_version: 9},
        %{api_key: 1, min_version: 0, max_version: 13}
      ])

    api_keys_binary =
      for %{api_key: key, min_version: min_v, max_version: max_v} <- api_keys, into: <<>> do
        <<key::16, min_v::16, max_v::16>>
      end

    <<
      correlation_id::32,
      error_code::16-signed,
      length(api_keys)::32,
      api_keys_binary::binary
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_code = Keyword.get(opts, :error_code, 0)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    api_keys =
      Keyword.get(opts, :api_keys, [
        %{api_key: 0, min_version: 0, max_version: 9},
        %{api_key: 1, min_version: 0, max_version: 13}
      ])

    api_keys_binary =
      for %{api_key: key, min_version: min_v, max_version: max_v} <- api_keys, into: <<>> do
        <<key::16, min_v::16, max_v::16>>
      end

    <<
      correlation_id::32,
      error_code::16-signed,
      length(api_keys)::32,
      api_keys_binary::binary,
      throttle_time_ms::32
    >>
  end

  def response_binary(2, opts),
    do: response_binary(1, Keyword.put_new(opts, :throttle_time_ms, 50))

  @doc """
  Builds a minimal valid response (empty api_keys).

  ## Options
  - `:correlation_id` - Default: 0
  - `:error_code` - Default: 0
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def minimal_response(version, opts \\ []) do
    response_binary(version, Keyword.put(opts, :api_keys, []))
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 35 (UNSUPPORTED_VERSION)
  - `:correlation_id` - Default: 0
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    opts
    |> Keyword.put_new(:error_code, 35)
    |> Keyword.put(:api_keys, [])
    |> then(&response_binary(version, &1))
  end

  @doc """
  Returns a large response with 10 APIs (captured from Kafka 7.4.0).
  """
  def large_response(1) do
    <<
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      10,
      0,
      0,
      0,
      0,
      0,
      9,
      0,
      1,
      0,
      0,
      0,
      13,
      0,
      2,
      0,
      0,
      0,
      7,
      0,
      3,
      0,
      0,
      0,
      12,
      0,
      4,
      0,
      0,
      0,
      7,
      0,
      5,
      0,
      0,
      0,
      4,
      0,
      6,
      0,
      0,
      0,
      8,
      0,
      7,
      0,
      0,
      0,
      3,
      0,
      8,
      0,
      0,
      0,
      8,
      0,
      9,
      0,
      0,
      0,
      8,
      0,
      0,
      0,
      0
    >>
  end

  @doc """
  Returns request data for V3 with empty fields (tests compact format).

  Useful for testing V3 flexible format with empty compact strings.
  """
  def v3_empty_fields_request_data do
    request = %Kayrock.ApiVersions.V3.Request{
      correlation_id: 0,
      client_id: "",
      client_software_name: "",
      client_software_version: ""
    }

    expected_binary = <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (3)
      0,
      3,
      # correlation_id
      0,
      0,
      0,
      0,
      # client_id length (0)
      0,
      0,
      # flexible header marker
      0,
      # empty compact_string (0+1)
      1,
      # empty compact_string (0+1)
      1,
      # tagged_fields count
      0
    >>

    {request, expected_binary}
  end
end
