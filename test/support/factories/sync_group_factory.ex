defmodule Kayrock.Test.Factories.SyncGroupFactory do
  @moduledoc """
  Factory for generating SyncGroup API test data (V0-V4).

  API Key: 14
  Used to: Distribute partition assignments to group members after a rebalance.

  Protocol changes by version:
  - V0: Basic sync with assignments
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
    request = %Kayrock.SyncGroup.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      generation_id: 1,
      member_id: "member-123",
      assignments: []
    }

    expected_binary = <<
      # api_key (14 = SyncGroup)
      0,
      14,
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
      "member-123"::binary,
      # assignments (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.SyncGroup.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      assignments: []
    }

    expected_binary = <<
      # api_key (14)
      0,
      14,
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
      1,
      # member_id
      0,
      6,
      "member"::binary,
      # assignments (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.SyncGroup.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      assignments: []
    }

    expected_binary = <<
      # api_key (14)
      0,
      14,
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
      # assignments (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.SyncGroup.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      group_instance_id: "static-instance",
      assignments: []
    }

    expected_binary = <<
      # api_key (14)
      0,
      14,
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
      15,
      "static-instance"::binary,
      # assignments (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.SyncGroup.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group",
      generation_id: 2,
      member_id: "member",
      group_instance_id: nil,
      assignments: [],
      tagged_fields: []
    }

    # V4 uses compact format
    expected_binary = <<
      # api_key (14)
      0,
      14,
      # api_version (4)
      0,
      4,
      # correlation_id
      0,
      0,
      0,
      4,
      # client_id
      0,
      4,
      "test"::binary,
      # flexible header marker (tagged fields)
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
      # assignments (compact array: length+1 = 1 for empty)
      1,
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
      iex> {actual, <<>>} = Kayrock.SyncGroup.V0.Response.deserialize(response_binary)
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
      0,
      # assignment length (10 bytes)
      0,
      0,
      0,
      10,
      # version (0)
      0,
      0,
      # partition_assignments (empty array)
      0,
      0,
      0,
      0,
      # user_data (empty bytes)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.SyncGroup.V0.Response{
      correlation_id: 0,
      error_code: 0,
      assignment: %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: <<0, 0, 0, 0>>
      }
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
      # throttle_time_ms (100)
      0,
      0,
      0,
      100,
      # error_code
      0,
      0,
      # assignment (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.SyncGroup.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 100,
      error_code: 0,
      assignment: %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: <<>>
      }
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
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0,
      # assignment (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.SyncGroup.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 50,
      error_code: 0,
      assignment: %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: <<>>
      }
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
      0,
      # assignment (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.SyncGroup.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      error_code: 0,
      assignment: %Kayrock.MemberAssignment{
        version: 0,
        partition_assignments: [],
        user_data: <<>>
      }
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
      # assignment (compact bytes: 0 = null)
      0,
      # tagged_fields
      0
    >>

    expected_struct = %Kayrock.SyncGroup.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      error_code: 0,
      assignment: nil,
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
  - `:assignment` - Default: nil (empty assignment)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    assignment = Keyword.get(opts, :assignment, :empty)

    assignment_bytes =
      case assignment do
        :empty ->
          <<
            # assignment length (10 bytes)
            0,
            0,
            0,
            10,
            # version (0)
            0,
            0,
            # partition_assignments (empty array)
            0,
            0,
            0,
            0,
            # user_data (empty bytes)
            0,
            0,
            0,
            0
          >>

        nil ->
          # Null assignment
          <<0, 0, 0, 0>>

        binary when is_binary(binary) ->
          binary
      end

    <<
      correlation_id::32,
      error_code::16-signed,
      assignment_bytes::binary
    >>
  end

  def response_binary(version, opts) when version in [1, 2, 3] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    assignment = Keyword.get(opts, :assignment, nil)

    assignment_bytes =
      case assignment do
        nil ->
          # Null assignment (length = 0)
          <<0, 0, 0, 0>>

        binary when is_binary(binary) ->
          binary
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed,
      assignment_bytes::binary
    >>
  end

  def response_binary(4, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 4)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    assignment = Keyword.get(opts, :assignment, nil)

    assignment_bytes =
      case assignment do
        nil ->
          # Compact nullable bytes: 0 = null
          <<0>>

        binary when is_binary(binary) ->
          binary
      end

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      error_code::16-signed,
      assignment_bytes::binary,
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
