defmodule Kayrock.Test.Factories.OffsetFetchFactory do
  @moduledoc """
  Factory for generating OffsetFetch API test data (V0-V6).

  API Key: 9
  Used to: Fetch committed offsets for a consumer group.

  Protocol changes by version:
  - V0-V1: Basic offset fetch with topic/partition structure
  - V2+: Adds group-level error_code in response
  - V3+: Adds throttle_time_ms in response
  - V5+: Adds committed_leader_epoch to partition response
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
    request = %Kayrock.OffsetFetch.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      topics: [
        %{
          name: "topic",
          partition_indexes: [0, 1, 2]
        }
      ]
    }

    expected_binary = <<
      # api_key (9 = OffsetFetch)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (3)
      0,
      0,
      0,
      3,
      # partition_index 0
      0,
      0,
      0,
      0,
      # partition_index 1
      0,
      0,
      0,
      1,
      # partition_index 2
      0,
      0,
      0,
      2
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.OffsetFetch.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "group",
      topics: [%{name: "topic", partition_indexes: [0]}]
    }

    expected_binary = <<
      # api_key (9)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (1)
      0,
      0,
      0,
      1,
      # partition_index 0
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.OffsetFetch.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "group",
      topics: [%{name: "topic", partition_indexes: [0]}]
    }

    expected_binary = <<
      # api_key (9)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (1)
      0,
      0,
      0,
      1,
      # partition_index 0
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.OffsetFetch.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group",
      topics: [%{name: "topic", partition_indexes: [0]}]
    }

    expected_binary = <<
      # api_key (9)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (1)
      0,
      0,
      0,
      1,
      # partition_index 0
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.OffsetFetch.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group",
      topics: [%{name: "topic", partition_indexes: [0]}]
    }

    expected_binary = <<
      # api_key (9)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (1)
      0,
      0,
      0,
      1,
      # partition_index 0
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.OffsetFetch.V5.Request{
      correlation_id: 5,
      client_id: "test",
      group_id: "group",
      topics: [%{name: "topic", partition_indexes: [0]}]
    }

    expected_binary = <<
      # api_key (9)
      0,
      9,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partition_indexes array length (1)
      0,
      0,
      0,
      1,
      # partition_index 0
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(6) do
    request = %Kayrock.OffsetFetch.V6.Request{
      correlation_id: 6,
      client_id: "test",
      group_id: "group",
      topics: [
        %{
          name: "topic",
          partition_indexes: [0],
          tagged_fields: []
        }
      ],
      tagged_fields: []
    }

    # V6 uses compact format
    expected_binary = <<
      # api_key (9)
      0,
      9,
      # api_version (6)
      0,
      6,
      # correlation_id
      0,
      0,
      0,
      6,
      # client_id
      0,
      4,
      "test"::binary,
      # flexible header marker (tagged fields)
      0,
      # group_id (compact string: length+1 = 6)
      6,
      "group"::binary,
      # topics compact array (length+1 = 2)
      2,
      # topic name (compact string: length+1 = 6)
      6,
      "topic"::binary,
      # partition_indexes compact array (length+1 = 2)
      2,
      # partition_index 0
      0,
      0,
      0,
      0,
      # topic tagged_fields
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

  ## Examples

      iex> {response_binary, expected_struct} = response_data(0)
      iex> {actual, <<>>} = Kayrock.OffsetFetch.V0.Response.deserialize(response_binary)
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
      0,
      # committed_offset (100)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # metadata (null)
      255,
      255,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V0.Response{
      correlation_id: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 100,
              metadata: nil,
              error_code: 0
            }
          ]
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
      0,
      # committed_offset (50)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      50,
      # metadata (non-null)
      0,
      11,
      "my-metadata"::binary,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V1.Response{
      correlation_id: 1,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 50,
              metadata: "my-metadata",
              error_code: 0
            }
          ]
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
      # topics array length (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V2.Response{
      correlation_id: 2,
      topics: [],
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
      # throttle_time_ms (100)
      0,
      0,
      0,
      100,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 100,
      topics: [],
      error_code: 0
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
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      topics: [],
      error_code: 0
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
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      5,
      "topic"::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition_index
      0,
      0,
      0,
      0,
      # committed_offset (200)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200,
      # committed_leader_epoch (10)
      0,
      0,
      0,
      10,
      # metadata (null)
      255,
      255,
      # partition error_code (0)
      0,
      0,
      # group-level error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 200,
              committed_leader_epoch: 10,
              metadata: nil,
              error_code: 0
            }
          ]
        }
      ],
      error_code: 0
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
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # topics compact array (length+1 = 2)
      2,
      # topic name (compact string: length+1 = 6)
      6,
      "topic"::binary,
      # partitions compact array (length+1 = 2)
      2,
      # partition_index
      0,
      0,
      0,
      0,
      # committed_offset (150)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      150,
      # committed_leader_epoch (5)
      0,
      0,
      0,
      5,
      # metadata (compact nullable string: 0 = null)
      0,
      # error_code (0)
      0,
      0,
      # partition tagged_fields
      0,
      # topic tagged_fields
      0,
      # error_code (0)
      0,
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.OffsetFetch.V6.Response{
      correlation_id: 6,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 150,
              committed_leader_epoch: 5,
              metadata: nil,
              error_code: 0,
              tagged_fields: []
            }
          ],
          tagged_fields: []
        }
      ],
      error_code: 0,
      tagged_fields: []
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary for the specified version with options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:throttle_time_ms` - V3+ only (default: 0)
  - `:topics` - List of topics with partitions (default: [])
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    topics = Keyword.get(opts, :topics, [])

    <<
      correlation_id::32,
      # topics array length
      length(topics)::32
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    topics = Keyword.get(opts, :topics, [])

    <<
      correlation_id::32,
      # topics array length
      length(topics)::32
    >>
  end

  def response_binary(2, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 2)
    topics = Keyword.get(opts, :topics, [])
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      # topics array length
      length(topics)::32,
      error_code::16-signed
    >>
  end

  def response_binary(version, opts) when version in [3, 4, 5] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    topics = Keyword.get(opts, :topics, [])
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # topics array length
      length(topics)::32,
      error_code::16-signed
    >>
  end

  def response_binary(6, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 6)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    topics = Keyword.get(opts, :topics, [])
    error_code = Keyword.get(opts, :error_code, 0)

    # V6 uses compact format
    topics_len = if topics == [], do: 1, else: length(topics) + 1

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      # topics compact array (length+1)
      topics_len,
      error_code::16-signed,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 16 (NOT_COORDINATOR)
  - `:correlation_id` - Default: based on version
  - `:throttle_time_ms` - V3+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 16)
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
