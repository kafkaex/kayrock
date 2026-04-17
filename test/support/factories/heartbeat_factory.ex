defmodule Kayrock.Test.Factories.HeartbeatFactory do
  @moduledoc """
  Factory for generating Heartbeat API test data (V0-V4).

  API Key: 12
  Used to: Keep consumer group membership alive between polls.

  Protocol changes by version:
  - V0: Basic heartbeat (group_id, generation_id, member_id)
  - V1+: Adds throttle_time_ms in response
  - V2: Same as V1
  - V3+: Adds group_instance_id for static membership
  - V4: Flexible/compact format with tagged_fields
  """

  # ============================================
  # Primary API - Request Data
  # ============================================

  @doc """
  Returns request test data for the specified version.

  Returns a tuple of `{struct, expected_binary}` where:
  - `struct` is the Request struct to serialize
  - `expected_binary` is the expected serialized output

  ## Examples

      iex> {request, expected_binary} = request_data(0)
      iex> serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      iex> assert serialized == expected_binary
  """
  def request_data(version)

  def request_data(0) do
    request = %Kayrock.Heartbeat.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      generation_id: 1,
      member_id: "member-123"
    }

    expected_binary = <<
      # api_key (12 = Heartbeat)
      0,
      12,
      # api_version (0)
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
      # group_id
      0,
      8,
      "my-group"::binary,
      # generation_id
      0,
      0,
      0,
      1,
      # member_id
      0,
      10,
      "member-123"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.Heartbeat.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "group",
      generation_id: 5,
      member_id: "member"
    }

    expected_binary = <<
      # api_key (12)
      0,
      12,
      # api_version (1)
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
      # group_id
      0,
      5,
      "group"::binary,
      # generation_id
      0,
      0,
      0,
      5,
      # member_id
      0,
      6,
      "member"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.Heartbeat.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "group-2",
      generation_id: 3,
      member_id: "member-2"
    }

    expected_binary = <<
      # api_key (12)
      0,
      12,
      # api_version (2)
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
      # group_id
      0,
      7,
      "group-2"::binary,
      # generation_id
      0,
      0,
      0,
      3,
      # member_id
      0,
      8,
      "member-2"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.Heartbeat.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      group_instance_id: "instance-1"
    }

    expected_binary = <<
      # api_key (12)
      0,
      12,
      # api_version (3)
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
      # group_id
      0,
      5,
      "group"::binary,
      # generation_id
      0,
      0,
      0,
      1,
      # member_id
      0,
      6,
      "member"::binary,
      # group_instance_id (nullable string, non-null)
      0,
      10,
      "instance-1"::binary
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.Heartbeat.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group",
      generation_id: 2,
      member_id: "member",
      group_instance_id: nil,
      tagged_fields: []
    }

    # V4 uses flexible format for body fields, but client_id in the header
    # ALWAYS uses int16-prefixed nullable_string (RequestHeader.json:
    # ClientId "flexibleVersions": "none"). Header v2 adds tag_buffer <<0>>.
    expected_binary = <<
      # api_key (12)
      0,
      12,
      # api_version (4)
      0,
      4,
      # correlation_id
      0,
      0,
      0,
      4,
      # client_id (int16-prefixed nullable_string, NOT compact)
      0,
      4,
      "test"::binary,
      # flexible header tag_buffer
      0,
      # group_id (compact string: length+1 = 6)
      6,
      "group"::binary,
      # generation_id
      0,
      0,
      0,
      2,
      # member_id (compact string: length+1 = 7)
      7,
      "member"::binary,
      # group_instance_id (compact nullable string: 0 = null)
      0,
      # tagged_fields
      0
    >>

    {request, expected_binary}
  end

  # ============================================
  # Primary API - Response Data
  # ============================================

  @doc """
  Returns response test data for the specified version.

  Returns a tuple of `{binary, expected_struct}` where:
  - `binary` is the response binary to deserialize
  - `expected_struct` is the expected deserialized struct

  ## Examples

      iex> {response_binary, expected_struct} = response_data(0)
      iex> {actual, <<>>} = Kayrock.Heartbeat.V0.Response.deserialize(response_binary)
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
      # error_code (0 = no error)
      0,
      0
    >>

    expected_struct = %Kayrock.Heartbeat.V0.Response{
      correlation_id: 0,
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
      # throttle_time_ms (50ms)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.Heartbeat.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      error_code: 0
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
      # throttle_time_ms (100ms)
      0,
      0,
      0,
      100,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.Heartbeat.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      error_code: 0
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
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.Heartbeat.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      error_code: 0
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    # V4 uses compact/flexible format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      4,
      # response header tagged_fields (varint: 0)
      0,
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # tagged_fields
      0
    >>

    expected_struct = %Kayrock.Heartbeat.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      error_code: 0,
      tagged_fields: []
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary for V0-V4 with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      error_code::16-signed
    >>
  end

  def response_binary(version, opts) when version in [1, 2, 3] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed
    >>
  end

  def response_binary(4, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 4)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      error_code::16-signed,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 27 (REBALANCE_IN_PROGRESS)
  - `:correlation_id` - Default: based on version
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 27)
    opts = Keyword.put(opts, :error_code, error_code)
    response_binary(version, opts)
  end

  # ============================================
  # Legacy API (backward compatibility)
  # ============================================

  @doc """
  Returns just the captured request binary for the specified version.
  """
  def captured_request_binary(version) do
    {_struct, binary} = request_data(version)
    binary
  end

  @doc """
  Returns just the captured response binary for the specified version.
  """
  def captured_response_binary(version) do
    {binary, _struct} = response_data(version)
    binary
  end
end
