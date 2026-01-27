defmodule Kayrock.Test.Factories.ListGroupsFactory do
  @moduledoc """
  Factory for generating ListGroups API test data (V0-V3).

  API Key: 16
  Used to: List all consumer groups on a broker.

  Protocol changes by version:
  - V0: Basic group listing (error_code, groups array)
  - V1+: Adds throttle_time_ms in response
  - V3: Compact/flexible format with tagged_fields
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
    request = %Kayrock.ListGroups.V0.Request{
      correlation_id: 0,
      client_id: "test"
    }

    expected_binary = <<
      # api_key (16 = ListGroups)
      0,
      16,
      # api_version (0)
      0,
      0,
      # correlation_id
      0,
      0,
      0,
      0,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.ListGroups.V1.Request{
      correlation_id: 1,
      client_id: "test"
    }

    expected_binary = <<
      # api_key (16 = ListGroups)
      0,
      16,
      # api_version (1)
      0,
      1,
      # correlation_id
      0,
      0,
      0,
      1,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.ListGroups.V2.Request{
      correlation_id: 2,
      client_id: "test"
    }

    expected_binary = <<
      # api_key (16 = ListGroups)
      0,
      16,
      # api_version (2)
      0,
      2,
      # correlation_id
      0,
      0,
      0,
      2,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.ListGroups.V3.Request{
      correlation_id: 3,
      client_id: "test",
      tagged_fields: []
    }

    # V3 uses compact format
    expected_binary = <<
      # api_key (16 = ListGroups)
      0,
      16,
      # api_version (3)
      0,
      3,
      # correlation_id
      0,
      0,
      0,
      3,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary,
      # flexible header tagged_fields (0)
      0,
      # request tagged_fields
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
      # error_code
      0,
      0,
      # groups array length (2)
      0,
      0,
      0,
      2,
      # group_id length
      0,
      6,
      # group_id
      "group1"::binary,
      # protocol_type length
      0,
      8,
      # protocol_type
      "consumer"::binary,
      # group_id length
      0,
      6,
      # group_id
      "group2"::binary,
      # protocol_type length
      0,
      8,
      # protocol_type
      "consumer"::binary
    >>

    expected_struct = %Kayrock.ListGroups.V0.Response{
      correlation_id: 0,
      error_code: 0,
      groups: [
        %{group_id: "group1", protocol_type: "consumer"},
        %{group_id: "group2", protocol_type: "consumer"}
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
      # throttle_time_ms (50)
      0,
      0,
      0,
      50,
      # error_code
      0,
      0,
      # groups array length (1)
      0,
      0,
      0,
      1,
      # group_id length
      0,
      5,
      # group_id
      "group"::binary,
      # protocol_type length
      0,
      8,
      # protocol_type
      "consumer"::binary
    >>

    expected_struct = %Kayrock.ListGroups.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      error_code: 0,
      groups: [
        %{group_id: "group", protocol_type: "consumer"}
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
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # groups array length (1)
      0,
      0,
      0,
      1,
      # group_id length
      0,
      5,
      # group_id
      "group"::binary,
      # protocol_type length
      0,
      8,
      # protocol_type
      "consumer"::binary
    >>

    expected_struct = %Kayrock.ListGroups.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 0,
      error_code: 0,
      groups: [
        %{group_id: "group", protocol_type: "consumer"}
      ]
    }

    {binary, expected_struct}
  end

  def response_data(3) do
    # V3 uses compact format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      3,
      # response header tagged_fields
      0,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # groups compact array (length+1 = 2)
      2,
      # group_id compact string (length+1 = 6)
      6,
      # group_id
      "group"::binary,
      # protocol_type compact string (length+1 = 9)
      9,
      # protocol_type
      "consumer"::binary,
      # group tagged_fields
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.ListGroups.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      error_code: 0,
      groups: [
        %{group_id: "group", protocol_type: "consumer", tagged_fields: []}
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
  - `:groups` - List of groups (default: single group)
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    groups = Keyword.get(opts, :groups, [%{group_id: "group", protocol_type: "consumer"}])

    groups_binary =
      for group <- groups, into: <<>> do
        group_id = Map.get(group, :group_id, "group")
        protocol_type = Map.get(group, :protocol_type, "consumer")

        <<byte_size(group_id)::16, group_id::binary, byte_size(protocol_type)::16,
          protocol_type::binary>>
      end

    <<
      correlation_id::32,
      error_code::16-signed,
      length(groups)::32,
      groups_binary::binary
    >>
  end

  def response_binary(version, opts) when version in [1, 2] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    groups = Keyword.get(opts, :groups, [%{group_id: "group", protocol_type: "consumer"}])

    groups_binary =
      for group <- groups, into: <<>> do
        group_id = Map.get(group, :group_id, "group")
        protocol_type = Map.get(group, :protocol_type, "consumer")

        <<byte_size(group_id)::16, group_id::binary, byte_size(protocol_type)::16,
          protocol_type::binary>>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      error_code::16-signed,
      length(groups)::32,
      groups_binary::binary
    >>
  end

  def response_binary(3, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 3)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    groups = Keyword.get(opts, :groups, [%{group_id: "group", protocol_type: "consumer"}])

    groups_binary =
      for group <- groups, into: <<>> do
        group_id = Map.get(group, :group_id, "group")
        protocol_type = Map.get(group, :protocol_type, "consumer")
        # Compact strings: length+1, and tagged_fields marker
        <<byte_size(group_id) + 1, group_id::binary, byte_size(protocol_type) + 1,
          protocol_type::binary, 0>>
      end

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      error_code::16-signed,
      # groups compact array: length+1
      length(groups) + 1,
      groups_binary::binary,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an empty response (no groups).

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0
  """
  def empty_response(version, opts \\ []) do
    response_binary(version, Keyword.put(opts, :groups, []))
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 29 (COORDINATOR_LOAD_IN_PROGRESS)
  - `:correlation_id` - Default: based on version
  """
  def error_response(version, opts \\ []) do
    opts
    |> Keyword.put_new(:error_code, 29)
    |> Keyword.put(:groups, [])
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
