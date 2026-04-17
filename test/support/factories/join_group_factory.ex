defmodule Kayrock.Test.Factories.JoinGroupFactory do
  @moduledoc """
  Factory for generating JoinGroup API test data (V0-V6).

  API Key: 11
  Used to: Join a consumer group and get initial membership assignment.

  Protocol changes by version:
  - V0: Basic join with session_timeout
  - V1+: Adds rebalance_timeout
  - V5+: Adds group_instance_id for static membership
  - V6: Flexible/compact format with tagged_fields
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
    request = %Kayrock.JoinGroup.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      session_timeout_ms: 30_000,
      member_id: "",
      protocol_type: "consumer",
      protocols: [
        %{
          name: "range",
          metadata: <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 0>>
        }
      ]
    }

    # Build expected metadata properly
    metadata = <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 0>>
    metadata_length = byte_size(metadata)

    expected_binary = <<
      # api_key (11 = JoinGroup)
      0,
      11,
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
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # member_id (empty)
      0,
      0,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length
      0,
      0,
      0,
      1,
      # protocol name
      0,
      5,
      "range"::binary,
      # protocol metadata length
      metadata_length::32,
      # protocol metadata
      metadata::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.JoinGroup.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      protocol_type: "consumer",
      protocols: []
    }

    expected_binary = <<
      # api_key (11)
      0,
      11,
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
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (empty)
      0,
      0,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.JoinGroup.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      protocol_type: "consumer",
      protocols: []
    }

    expected_binary = <<
      # api_key (11)
      0,
      11,
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
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (empty)
      0,
      0,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.JoinGroup.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      protocol_type: "consumer",
      protocols: []
    }

    expected_binary = <<
      # api_key (11)
      0,
      11,
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
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (empty)
      0,
      0,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.JoinGroup.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      protocol_type: "consumer",
      protocols: []
    }

    expected_binary = <<
      # api_key (11)
      0,
      11,
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
      # group_id
      0,
      5,
      "group"::binary,
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (empty)
      0,
      0,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.JoinGroup.V5.Request{
      correlation_id: 5,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      group_instance_id: "static-instance-1",
      protocol_type: "consumer",
      protocols: []
    }

    expected_binary = <<
      # api_key (11)
      0,
      11,
      # api_version (5)
      0,
      5,
      # correlation_id
      0,
      0,
      0,
      5,
      # client_id
      0,
      4,
      "test"::binary,
      # group_id
      0,
      5,
      "group"::binary,
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (empty)
      0,
      0,
      # group_instance_id (nullable string, non-null)
      0,
      17,
      "static-instance-1"::binary,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocols array length (empty)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(6) do
    request = %Kayrock.JoinGroup.V6.Request{
      correlation_id: 6,
      client_id: "test",
      group_id: "group",
      session_timeout_ms: 30_000,
      rebalance_timeout_ms: 60_000,
      member_id: "",
      group_instance_id: nil,
      protocol_type: "consumer",
      protocols: [],
      tagged_fields: []
    }

    # V6 uses compact format
    expected_binary = <<
      # api_key (11)
      0,
      11,
      # api_version (6)
      0,
      6,
      # correlation_id
      0,
      0,
      0,
      6,
      # client_id (int16-prefixed nullable_string)
      0,
      4,
      "test"::binary,
      # flexible header tag_buffer
      0,
      # group_id (compact string: length+1 = 6)
      6,
      "group"::binary,
      # session_timeout_ms
      0,
      0,
      117,
      48,
      # rebalance_timeout_ms
      0,
      0,
      234,
      96,
      # member_id (compact string: length+1 = 1 for empty string)
      1,
      # group_instance_id (compact nullable string: 0 = null)
      0,
      # protocol_type (compact string: length+1 = 9)
      9,
      "consumer"::binary,
      # protocols (compact array: length+1 = 1 for empty array)
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
      iex> {actual, <<>>} = Kayrock.JoinGroup.V0.Response.deserialize(response_binary)
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
      # generation_id
      0,
      0,
      0,
      1,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty for non-leader)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V0.Response{
      correlation_id: 0,
      error_code: 0,
      generation_id: 1,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
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
      # generation_id
      0,
      0,
      0,
      2,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V1.Response{
      correlation_id: 1,
      error_code: 0,
      generation_id: 2,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
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
      # throttle_time_ms (50ms)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0,
      # generation_id
      0,
      0,
      0,
      3,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 50,
      error_code: 0,
      generation_id: 3,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
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
      # throttle_time_ms (100ms)
      0,
      0,
      0,
      100,
      # error_code
      0,
      0,
      # generation_id
      0,
      0,
      0,
      4,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 100,
      error_code: 0,
      generation_id: 4,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
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
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # generation_id
      0,
      0,
      0,
      5,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      error_code: 0,
      generation_id: 5,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
    }

    {binary, expected_struct}
  end

  def response_data(5) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      5,
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # generation_id
      0,
      0,
      0,
      6,
      # protocol_name
      0,
      5,
      "range"::binary,
      # leader
      0,
      6,
      "leader"::binary,
      # member_id
      0,
      9,
      "member-id"::binary,
      # members array (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      error_code: 0,
      generation_id: 6,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: []
    }

    {binary, expected_struct}
  end

  def response_data(6) do
    # V6 uses compact/flexible format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      6,
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
      # generation_id
      0,
      0,
      0,
      7,
      # protocol_name (compact string: length+1 = 6)
      6,
      "range"::binary,
      # leader (compact string: length+1 = 7)
      7,
      "leader"::binary,
      # member_id (compact string: length+1 = 10)
      10,
      "member-id"::binary,
      # members (compact array: length+1 = 1 for empty array)
      1,
      # tagged_fields
      0
    >>

    expected_struct = %Kayrock.JoinGroup.V6.Response{
      correlation_id: 6,
      throttle_time_ms: 0,
      error_code: 0,
      generation_id: 7,
      protocol_name: "range",
      leader: "leader",
      member_id: "member-id",
      members: [],
      tagged_fields: []
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary for V0-V6 with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:generation_id` - Default: based on version
  - `:protocol_name` - Default: "range"
  - `:leader` - Default: "leader"
  - `:member_id` - Default: "member-id"
  - `:throttle_time_ms` - V2+ only (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    generation_id = Keyword.get(opts, :generation_id, 1)
    protocol_name = Keyword.get(opts, :protocol_name, "range")
    leader = Keyword.get(opts, :leader, "leader")
    member_id = Keyword.get(opts, :member_id, "member-id")

    <<
      correlation_id::32,
      error_code::16-signed,
      generation_id::32-signed,
      byte_size(protocol_name)::16,
      protocol_name::binary,
      byte_size(leader)::16,
      leader::binary,
      byte_size(member_id)::16,
      member_id::binary,
      # empty members array
      0::32
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_code = Keyword.get(opts, :error_code, 0)
    generation_id = Keyword.get(opts, :generation_id, 2)
    protocol_name = Keyword.get(opts, :protocol_name, "range")
    leader = Keyword.get(opts, :leader, "leader")
    member_id = Keyword.get(opts, :member_id, "member-id")

    <<
      correlation_id::32,
      error_code::16-signed,
      generation_id::32-signed,
      byte_size(protocol_name)::16,
      protocol_name::binary,
      byte_size(leader)::16,
      leader::binary,
      byte_size(member_id)::16,
      member_id::binary,
      # empty members array
      0::32
    >>
  end

  def response_binary(version, opts) when version in [2, 3, 4, 5] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    generation_id = Keyword.get(opts, :generation_id, version + 1)
    protocol_name = Keyword.get(opts, :protocol_name, "range")
    leader = Keyword.get(opts, :leader, "leader")
    member_id = Keyword.get(opts, :member_id, "member-id")

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed,
      generation_id::32-signed,
      byte_size(protocol_name)::16,
      protocol_name::binary,
      byte_size(leader)::16,
      leader::binary,
      byte_size(member_id)::16,
      member_id::binary,
      # empty members array
      0::32
    >>
  end

  def response_binary(6, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 6)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    generation_id = Keyword.get(opts, :generation_id, 7)
    protocol_name = Keyword.get(opts, :protocol_name, "range")
    leader = Keyword.get(opts, :leader, "leader")
    member_id = Keyword.get(opts, :member_id, "member-id")

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      error_code::16-signed,
      generation_id::32-signed,
      # compact string encoding
      byte_size(protocol_name) + 1,
      protocol_name::binary,
      byte_size(leader) + 1,
      leader::binary,
      byte_size(member_id) + 1,
      member_id::binary,
      # empty members array (compact: 1 = empty)
      1,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 25 (UNKNOWN_MEMBER_ID)
  - `:correlation_id` - Default: based on version
  - `:generation_id` - Default: -1
  - `:protocol_name` - Default: ""
  - `:leader` - Default: ""
  - `:member_id` - Default: ""
  - `:throttle_time_ms` - V2+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 25)
    generation_id = Keyword.get(opts, :generation_id, -1)

    opts =
      opts
      |> Keyword.put(:error_code, error_code)
      |> Keyword.put(:generation_id, generation_id)
      |> Keyword.put_new(:protocol_name, "")
      |> Keyword.put_new(:leader, "")
      |> Keyword.put_new(:member_id, "")

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
