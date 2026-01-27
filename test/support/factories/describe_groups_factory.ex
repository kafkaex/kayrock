defmodule Kayrock.Test.Factories.DescribeGroupsFactory do
  @moduledoc """
  Factory for generating DescribeGroups API test data (V0-V5).

  API Key: 15
  Used to: Get detailed information about consumer groups.

  Protocol changes by version:
  - V0: Basic group description
  - V1+: Adds throttle_time_ms in response
  - V2: Same as V1
  - V3+: Adds include_authorized_operations and authorized_operations
  - V4: Adds group_instance_id for static membership
  - V5: Flexible/compact format with tagged_fields
  """

  # ============================================
  # Primary API - Request Data
  # ============================================

  @doc """
  Returns request test data for the specified version.

  Returns a tuple of `{struct, expected_binary}` where:
  - `struct` is the Request struct to serialize
  - `expected_binary` is the expected serialized output
  """
  def request_data(version)

  def request_data(0) do
    request = %Kayrock.DescribeGroups.V0.Request{
      correlation_id: 0,
      client_id: "test",
      groups: ["group1", "group2"]
    }

    expected_binary = <<
      # api_key (15 = DescribeGroups)
      0,
      15,
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
      # groups array length
      0,
      0,
      0,
      2,
      # group 1
      0,
      6,
      "group1"::binary,
      # group 2
      0,
      6,
      "group2"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DescribeGroups.V1.Request{
      correlation_id: 1,
      client_id: "test",
      groups: ["my-group"]
    }

    expected_binary = <<
      # api_key (15)
      0,
      15,
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
      # groups array length
      0,
      0,
      0,
      1,
      # group
      0,
      8,
      "my-group"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.DescribeGroups.V2.Request{
      correlation_id: 2,
      client_id: "test",
      groups: ["group"]
    }

    expected_binary = <<
      # api_key (15)
      0,
      15,
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
      # groups array length
      0,
      0,
      0,
      1,
      # group
      0,
      5,
      "group"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.DescribeGroups.V3.Request{
      correlation_id: 3,
      client_id: "test",
      groups: ["group"],
      include_authorized_operations: false
    }

    expected_binary = <<
      # api_key (15)
      0,
      15,
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
      # groups array length
      0,
      0,
      0,
      1,
      # group
      0,
      5,
      "group"::binary,
      # include_authorized_operations (false = 0)
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.DescribeGroups.V4.Request{
      correlation_id: 4,
      client_id: "test",
      groups: ["group"],
      include_authorized_operations: true
    }

    expected_binary = <<
      # api_key (15)
      0,
      15,
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
      # groups array length
      0,
      0,
      0,
      1,
      # group
      0,
      5,
      "group"::binary,
      # include_authorized_operations (true = 1)
      1
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.DescribeGroups.V5.Request{
      correlation_id: 5,
      client_id: "test",
      groups: ["group"],
      include_authorized_operations: false,
      tagged_fields: []
    }

    # V5 uses compact format
    expected_binary = <<
      # api_key (15)
      0,
      15,
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
      # flexible header tagged_fields
      0,
      # groups compact array (length+1 = 2)
      2,
      # group compact string (length+1 = 6)
      6,
      "group"::binary,
      # include_authorized_operations (false = 0)
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
  """
  def response_data(version)

  def response_data(0) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      0,
      # groups array length
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # group_id
      0,
      8,
      "my-group"::binary,
      # state
      0,
      6,
      "Stable"::binary,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocol
      0,
      5,
      "range"::binary,
      # members (empty)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeGroups.V0.Response{
      correlation_id: 0,
      groups: [
        %{
          error_code: 0,
          group_id: "my-group",
          group_state: "Stable",
          protocol_type: "consumer",
          protocol_data: "range",
          members: []
        }
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
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # groups
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # group_id
      0,
      5,
      "group"::binary,
      # state
      0,
      5,
      "Empty"::binary,
      # protocol_type
      0,
      0,
      # protocol
      0,
      0,
      # members
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeGroups.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 100,
      groups: [
        %{
          error_code: 0,
          group_id: "group",
          group_state: "Empty",
          protocol_type: "",
          protocol_data: "",
          members: []
        }
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
      # throttle_time_ms
      0,
      0,
      0,
      50,
      # groups
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # group_id
      0,
      5,
      "group"::binary,
      # state
      0,
      6,
      "Stable"::binary,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocol
      0,
      5,
      "range"::binary,
      # members
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeGroups.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 50,
      groups: [
        %{
          error_code: 0,
          group_id: "group",
          group_state: "Stable",
          protocol_type: "consumer",
          protocol_data: "range",
          members: []
        }
      ]
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
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # groups
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # group_id
      0,
      5,
      "group"::binary,
      # state
      0,
      6,
      "Stable"::binary,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocol
      0,
      5,
      "range"::binary,
      # members
      0,
      0,
      0,
      0,
      # authorized_operations (-2147483648 = INT32_MIN means not included)
      255,
      255,
      255,
      255
    >>

    expected_struct = %Kayrock.DescribeGroups.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      groups: [
        %{
          error_code: 0,
          group_id: "group",
          group_state: "Stable",
          protocol_type: "consumer",
          protocol_data: "range",
          members: [],
          authorized_operations: -1
        }
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
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # groups
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # group_id
      0,
      5,
      "group"::binary,
      # state
      0,
      6,
      "Stable"::binary,
      # protocol_type
      0,
      8,
      "consumer"::binary,
      # protocol
      0,
      5,
      "range"::binary,
      # members (1 member)
      0,
      0,
      0,
      1,
      # member_id
      0,
      6,
      "member"::binary,
      # group_instance_id (nullable, null = -1)
      255,
      255,
      # client_id
      0,
      6,
      "client"::binary,
      # client_host
      0,
      9,
      "localhost"::binary,
      # member_metadata (4 bytes)
      0,
      0,
      0,
      4,
      1,
      2,
      3,
      4,
      # member_assignment (4 bytes)
      0,
      0,
      0,
      4,
      5,
      6,
      7,
      8,
      # authorized_operations
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.DescribeGroups.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      groups: [
        %{
          error_code: 0,
          group_id: "group",
          group_state: "Stable",
          protocol_type: "consumer",
          protocol_data: "range",
          members: [
            %{
              member_id: "member",
              group_instance_id: nil,
              client_id: "client",
              client_host: "localhost",
              member_metadata: <<1, 2, 3, 4>>,
              member_assignment: <<5, 6, 7, 8>>
            }
          ],
          authorized_operations: 0
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(5) do
    # V5 uses compact format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      5,
      # response header tagged_fields
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # groups compact array (length+1 = 2)
      2,
      # error_code
      0,
      0,
      # group_id compact string (length+1 = 6)
      6,
      "group"::binary,
      # state compact string (length+1 = 7)
      7,
      "Stable"::binary,
      # protocol_type compact string (length+1 = 9)
      9,
      "consumer"::binary,
      # protocol compact string (length+1 = 6)
      6,
      "range"::binary,
      # members compact array (length+1 = 1, empty)
      1,
      # authorized_operations
      0,
      0,
      0,
      0,
      # group tagged_fields
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.DescribeGroups.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      groups: [
        %{
          error_code: 0,
          group_id: "group",
          group_state: "Stable",
          protocol_type: "consumer",
          protocol_data: "range",
          members: [],
          authorized_operations: 0,
          tagged_fields: []
        }
      ],
      tagged_fields: []
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0
  - `:group_id` - Default: "group"
  - `:group_state` - Default: "Empty"
  - `:throttle_time_ms` - V1+ only (default: 0)
  - `:with_members` - Include member data (default: false)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    group_id = Keyword.get(opts, :group_id, "group")
    group_state = Keyword.get(opts, :group_state, "Empty")

    <<
      correlation_id::32,
      # groups array length (1)
      0,
      0,
      0,
      1,
      error_code::16-signed,
      byte_size(group_id)::16,
      group_id::binary,
      byte_size(group_state)::16,
      group_state::binary,
      # protocol_type (empty)
      0,
      0,
      # protocol (empty)
      0,
      0,
      # members (empty)
      0,
      0,
      0,
      0
    >>
  end

  def response_binary(version, opts) when version in [1, 2] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    group_id = Keyword.get(opts, :group_id, "group")
    group_state = Keyword.get(opts, :group_state, "Empty")

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # groups array length (1)
      0,
      0,
      0,
      1,
      error_code::16-signed,
      byte_size(group_id)::16,
      group_id::binary,
      byte_size(group_state)::16,
      group_state::binary,
      # protocol_type (empty)
      0,
      0,
      # protocol (empty)
      0,
      0,
      # members (empty)
      0,
      0,
      0,
      0
    >>
  end

  def response_binary(version, opts) when version in [3, 4] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    group_id = Keyword.get(opts, :group_id, "group")
    group_state = Keyword.get(opts, :group_state, "Empty")

    members_binary =
      if version == 4 do
        # V4 includes group_instance_id
        <<
          # members (empty)
          0,
          0,
          0,
          0
        >>
      else
        <<
          # members (empty)
          0,
          0,
          0,
          0
        >>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # groups array length (1)
      0,
      0,
      0,
      1,
      error_code::16-signed,
      byte_size(group_id)::16,
      group_id::binary,
      byte_size(group_state)::16,
      group_state::binary,
      # protocol_type (empty)
      0,
      0,
      # protocol (empty)
      0,
      0,
      members_binary::binary,
      # authorized_operations (-1 = not included)
      255,
      255,
      255,
      255
    >>
  end

  def response_binary(5, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 5)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    group_id = Keyword.get(opts, :group_id, "group")
    group_state = Keyword.get(opts, :group_state, "Empty")

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      # groups compact array (length+1 = 2)
      2,
      error_code::16-signed,
      # group_id compact string (length+1)
      byte_size(group_id) + 1,
      group_id::binary,
      # group_state compact string (length+1)
      byte_size(group_state) + 1,
      group_state::binary,
      # protocol_type compact string (empty, length+1 = 1)
      1,
      # protocol compact string (empty, length+1 = 1)
      1,
      # members compact array (empty, length+1 = 1)
      1,
      # authorized_operations (-1 = not included)
      255,
      255,
      255,
      255,
      # group tagged_fields
      0,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 16 (NOT_COORDINATOR)
  - `:correlation_id` - Default: based on version
  """
  def error_response(version, opts \\ []) do
    opts
    |> Keyword.put_new(:error_code, 16)
    |> then(&response_binary(version, &1))
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
