defmodule Kayrock.Test.Factories.OffsetCommitFactory do
  @moduledoc """
  Factory for generating OffsetCommit API test data (V0-V8).

  API Key: 8
  Used to: Commit consumer offsets to Kafka.

  Protocol changes by version:
  - V0: Basic offset commit (group_id, topics with partitions)
  - V1: Adds generation_id, member_id, commit_timestamp
  - V2+: Adds retention_time_ms (no commit_timestamp)
  - V3+: Adds throttle_time_ms in response
  - V5: Removes retention_time_ms (deprecated)
  - V6+: Adds committed_leader_epoch
  - V7+: Adds group_instance_id (static membership)
  - V8: Flexible/compact format with tagged_fields
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
    request = %Kayrock.OffsetCommit.V0.Request{
      correlation_id: 0,
      client_id: "test",
      group_id: "my-group",
      topics: [
        %{
          name: "topic",
          partitions: [
            %{
              partition_index: 0,
              committed_offset: 100,
              committed_metadata: ""
            }
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (8 = OffsetCommit)
      0,
      8,
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
      # committed_offset
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100,
      # committed_metadata (empty string)
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.OffsetCommit.V1.Request{
      correlation_id: 1,
      client_id: "test",
      group_id: "my-group",
      generation_id: 5,
      member_id: "member-123",
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      8,
      "my-group"::binary,
      # generation_id
      0,
      0,
      0,
      5,
      # member_id
      0,
      10,
      "member-123"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.OffsetCommit.V2.Request{
      correlation_id: 2,
      client_id: "test",
      group_id: "my-group",
      generation_id: 5,
      member_id: "member-123",
      retention_time_ms: 86_400_000,
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      8,
      "my-group"::binary,
      # generation_id
      0,
      0,
      0,
      5,
      # member_id
      0,
      10,
      "member-123"::binary,
      # retention_time_ms (86400000 = 24 hours)
      0,
      0,
      0,
      0,
      5,
      38,
      92,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.OffsetCommit.V3.Request{
      correlation_id: 3,
      client_id: "test",
      group_id: "group-3",
      generation_id: 2,
      member_id: "member-3",
      retention_time_ms: -1,
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      7,
      "group-3"::binary,
      # generation_id
      0,
      0,
      0,
      2,
      # member_id
      0,
      8,
      "member-3"::binary,
      # retention_time_ms (-1 = use broker default)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.OffsetCommit.V4.Request{
      correlation_id: 4,
      client_id: "test",
      group_id: "group-4",
      generation_id: 1,
      member_id: "member-4",
      retention_time_ms: 3_600_000,
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      7,
      "group-4"::binary,
      # generation_id
      0,
      0,
      0,
      1,
      # member_id
      0,
      8,
      "member-4"::binary,
      # retention_time_ms (3600000 = 1 hour)
      0,
      0,
      0,
      0,
      0,
      54,
      238,
      128,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.OffsetCommit.V5.Request{
      correlation_id: 5,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      # generation_id
      0,
      0,
      0,
      1,
      # member_id
      0,
      6,
      "member"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(6) do
    request = %Kayrock.OffsetCommit.V6.Request{
      correlation_id: 6,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
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
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(7) do
    request = %Kayrock.OffsetCommit.V7.Request{
      correlation_id: 7,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      group_instance_id: nil,
      topics: []
    }

    expected_binary = <<
      # api_key (8)
      0,
      8,
      # api_version (7)
      0,
      7,
      # correlation_id
      0,
      0,
      0,
      7,
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
      # group_instance_id (null)
      255,
      255,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(8) do
    request = %Kayrock.OffsetCommit.V8.Request{
      correlation_id: 8,
      client_id: "test",
      group_id: "group",
      generation_id: 1,
      member_id: "member",
      group_instance_id: nil,
      topics: [],
      tagged_fields: []
    }

    # V8 uses compact format
    expected_binary = <<
      # api_key (8)
      0,
      8,
      # api_version (8)
      0,
      8,
      # correlation_id
      0,
      0,
      0,
      8,
      # client_id (int16-prefixed nullable_string)
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
      1,
      # member_id (compact string: length+1 = 7)
      7,
      "member"::binary,
      # group_instance_id (compact nullable string: 0 = null)
      0,
      # topics (compact array: length+1 = 1 for empty)
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
      iex> {actual, <<>>} = Kayrock.OffsetCommit.V0.Response.deserialize(response_binary)
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
      # error_code (0 = no error)
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V0.Response{
      correlation_id: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V1.Response{
      correlation_id: 1,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V2.Response{
      correlation_id: 2,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
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
      # throttle_time_ms (100ms)
      0,
      0,
      0,
      100,
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 100,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
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
      # throttle_time_ms (50ms)
      0,
      0,
      0,
      50,
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 50,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
        }
      ]
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(6) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      6,
      # throttle_time_ms (0ms)
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V6.Response{
      correlation_id: 6,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(7) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      7,
      # throttle_time_ms (0ms)
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
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V7.Response{
      correlation_id: 7,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(8) do
    # V8 uses compact/flexible format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      8,
      # response header tagged_fields (varint: 0)
      0,
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # topics (compact array: length+1 = 2 for 1 topic)
      2,
      # topic name (compact string: length+1 = 6)
      6,
      "topic"::binary,
      # partitions (compact array: length+1 = 2 for 1 partition)
      2,
      # partition_index
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # partition tagged_fields
      0,
      # topic tagged_fields
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.OffsetCommit.V8.Response{
      correlation_id: 8,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          partitions: [
            %{partition_index: 0, error_code: 0, tagged_fields: []}
          ],
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
  Builds a custom response binary for V0-V8 with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:throttle_time_ms` - V3+ only (default: 0)
  - `:topic` - Default: "topic"
  - `:partition_index` - Default: 0
  """
  def response_binary(version, opts \\ [])

  def response_binary(version, opts) when version in [0, 1, 2] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    error_code = Keyword.get(opts, :error_code, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition_index = Keyword.get(opts, :partition_index, 0)

    <<
      correlation_id::32,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      byte_size(topic)::16,
      topic::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition_index
      partition_index::32,
      # error_code
      error_code::16-signed
    >>
  end

  def response_binary(version, opts) when version in [3, 4, 5, 6, 7] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition_index = Keyword.get(opts, :partition_index, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      byte_size(topic)::16,
      topic::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition_index
      partition_index::32,
      # error_code
      error_code::16-signed
    >>
  end

  def response_binary(8, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 8)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition_index = Keyword.get(opts, :partition_index, 0)

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      # topics (compact array: length+1 = 2 for 1 topic)
      2,
      # topic name (compact string: length+1)
      byte_size(topic) + 1,
      topic::binary,
      # partitions (compact array: length+1 = 2 for 1 partition)
      2,
      # partition_index
      partition_index::32,
      # error_code
      error_code::16-signed,
      # partition tagged_fields
      0,
      # topic tagged_fields
      0,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 25 (UNKNOWN_MEMBER_ID)
  - `:correlation_id` - Default: based on version
  - `:throttle_time_ms` - V3+ only (default: 0)
  - `:topic` - Default: "topic"
  - `:partition_index` - Default: 0
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 25)
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
