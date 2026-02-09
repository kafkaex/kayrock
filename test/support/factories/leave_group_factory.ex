defmodule Kayrock.Test.Factories.LeaveGroupFactory do
  @moduledoc """
  Factory for generating LeaveGroup test data.

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
    request = %Kayrock.LeaveGroup.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      member_id: "member-123"
    }

    expected_binary = <<
      # api_key (13 = LeaveGroup)
      0,
      13,
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
      # member_id
      0,
      10,
      "member-123"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.LeaveGroup.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "group",
      member_id: "member"
    }

    expected_binary = <<
      # api_key (13 = LeaveGroup)
      0,
      13,
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
      # member_id
      0,
      6,
      "member"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.LeaveGroup.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "group",
      member_id: "member"
    }

    expected_binary = <<
      # api_key (13 = LeaveGroup)
      0,
      13,
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
      # member_id
      0,
      6,
      "member"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.LeaveGroup.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group",
      members: [
        %{member_id: "member-1", group_instance_id: nil},
        %{member_id: "member-2", group_instance_id: "static-1"}
      ]
    }

    expected_binary = <<
      # api_key (13 = LeaveGroup)
      0,
      13,
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
      # members array length (2)
      0,
      0,
      0,
      2,
      # member 1
      # member_id
      0,
      8,
      "member-1"::binary,
      # group_instance_id (null)
      255,
      255,
      # member 2
      # member_id
      0,
      8,
      "member-2"::binary,
      # group_instance_id
      0,
      8,
      "static-1"::binary
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.LeaveGroup.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group",
      members: [
        %{member_id: "member", group_instance_id: nil, tagged_fields: []}
      ],
      tagged_fields: []
    }

    expected_binary = <<
      # api_key (13 = LeaveGroup)
      0,
      13,
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
      # flexible header marker
      0,
      # group_id (compact string: length+1)
      6,
      "group"::binary,
      # members (compact array: length+1 = 2 for 1 element)
      2,
      # member 1
      # member_id (compact string)
      7,
      "member"::binary,
      # group_instance_id (compact nullable: 0 = null)
      0,
      # member tagged_fields
      0,
      # root tagged_fields
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
      iex> {actual, <<>>} = Kayrock.LeaveGroup.V0.Response.deserialize(response_binary)
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

    expected_struct = %Kayrock.LeaveGroup.V0.Response{
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
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.LeaveGroup.V1.Response{
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
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.LeaveGroup.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 50,
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
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0,
      # members array length (2)
      0,
      0,
      0,
      2,
      # member 1
      # member_id
      0,
      8,
      "member-1"::binary,
      # group_instance_id (null)
      255,
      255,
      # error_code
      0,
      0,
      # member 2
      # member_id
      0,
      8,
      "member-2"::binary,
      # group_instance_id
      0,
      8,
      "static-1"::binary,
      # error_code
      0,
      25
    >>

    expected_struct = %Kayrock.LeaveGroup.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 50,
      error_code: 0,
      members: [
        %{member_id: "member-1", group_instance_id: nil, error_code: 0},
        %{member_id: "member-2", group_instance_id: "static-1", error_code: 25}
      ]
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      4,
      # flexible header tagged_fields
      0,
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0,
      # members (compact array: length+1 = 2 for 1 element)
      2,
      # member 1
      # member_id (compact string)
      7,
      "member"::binary,
      # group_instance_id (compact nullable: 0 = null)
      0,
      # error_code
      0,
      0,
      # member tagged_fields
      0,
      # root tagged_fields
      0
    >>

    expected_struct = %Kayrock.LeaveGroup.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 50,
      error_code: 0,
      members: [
        %{member_id: "member", group_instance_id: nil, error_code: 0, tagged_fields: []}
      ],
      tagged_fields: []
    }

    {binary, expected_struct}
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

  ## Options (version-specific)
  - `:correlation_id` - Default: version number
  - `:error_code` - Default: 0
  - `:throttle_time_ms` - V1+ only (default: 0)
  - `:members` - V3+ only (list of member maps, default: [])

  ## Examples

      iex> response_binary(0, error_code: 25)
      # V0 response with UNKNOWN_MEMBER_ID error

      iex> response_binary(1, throttle_time_ms: 100, correlation_id: 42)
      # V1 response with custom throttle and correlation_id

      iex> response_binary(3, members: [%{member_id: "m1", group_instance_id: nil, error_code: 0}])
      # V3 response with custom members
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

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed
    >>
  end

  def response_binary(2, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 2)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed
    >>
  end

  def response_binary(3, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 3)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    members = Keyword.get(opts, :members, [])

    members_binary =
      for %{member_id: mid, group_instance_id: gid, error_code: ec} <- members, into: <<>> do
        member_id_bin = <<byte_size(mid)::16, mid::binary>>

        group_instance_id_bin =
          if gid == nil do
            <<-1::16-signed>>
          else
            <<byte_size(gid)::16, gid::binary>>
          end

        <<member_id_bin::binary, group_instance_id_bin::binary, ec::16-signed>>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed,
      length(members)::32,
      members_binary::binary
    >>
  end

  def response_binary(4, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 4)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    members = Keyword.get(opts, :members, [])

    members_binary =
      for %{member_id: mid, group_instance_id: gid, error_code: ec} <- members, into: <<>> do
        # compact string: length + 1
        member_id_bin = <<byte_size(mid) + 1, mid::binary>>

        # compact nullable: 0 for null, length+1 otherwise
        group_instance_id_bin =
          if gid == nil do
            <<0>>
          else
            <<byte_size(gid) + 1, gid::binary>>
          end

        # member tagged_fields (empty)
        <<member_id_bin::binary, group_instance_id_bin::binary, ec::16-signed, 0>>
      end

    # compact array: length+1 encoding (0 = null, 1 = empty, 2+ = length+1)
    members_length_encoded =
      if members == [] do
        <<1>>
      else
        <<length(members) + 1>>
      end

    <<
      correlation_id::32,
      # flexible header tagged_fields
      0,
      throttle_time_ms::32,
      error_code::16-signed,
      members_length_encoded::binary,
      members_binary::binary,
      # root tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 25 (UNKNOWN_MEMBER_ID)
  - `:correlation_id` - Default: version number
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    opts
    |> Keyword.put_new(:error_code, 25)
    |> then(&response_binary(version, &1))
  end
end
