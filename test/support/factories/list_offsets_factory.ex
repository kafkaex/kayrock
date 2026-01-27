defmodule Kayrock.Test.Factories.ListOffsetsFactory do
  @moduledoc """
  Factory for generating ListOffsets API test data (V0-V5).

  API Key: 2
  Used to: Get available offsets for topic partitions.

  Protocol changes by version:
  - V0: Basic offset fetch with max_num_offsets, returns array of offsets
  - V1+: Removes max_num_offsets, returns single offset + timestamp
  - V2+: Adds isolation_level and throttle_time_ms in response
  - V3: Same as V2
  - V4+: Adds current_leader_epoch to request and leader_epoch to response
  - V5: Same as V4
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
    request = %Kayrock.ListOffsets.V0.Request{
      correlation_id: 0,
      client_id: "test",
      replica_id: -1,
      topics: [
        %{
          topic: "topic",
          partitions: [
            %{
              partition: 0,
              timestamp: -1,
              max_num_offsets: 1
            }
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2 = ListOffsets)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
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
      # partition (0)
      0,
      0,
      0,
      0,
      # timestamp (-1 = latest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # max_num_offsets (1)
      0,
      0,
      0,
      1
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.ListOffsets.V1.Request{
      correlation_id: 1,
      client_id: "test",
      replica_id: -1,
      topics: [
        %{
          topic: "topic",
          partitions: [
            %{partition: 0, timestamp: -1}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
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
      # partition (0)
      0,
      0,
      0,
      0,
      # timestamp (-1 = latest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.ListOffsets.V2.Request{
      correlation_id: 2,
      client_id: "test",
      replica_id: -1,
      isolation_level: 0,
      topics: [
        %{
          topic: "topic",
          partitions: [
            %{partition: 0, timestamp: -2}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # isolation_level (0 = read_uncommitted)
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
      # partition (0)
      0,
      0,
      0,
      0,
      # timestamp (-2 = earliest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      254
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.ListOffsets.V3.Request{
      correlation_id: 3,
      client_id: "test",
      replica_id: -1,
      isolation_level: 1,
      topics: [
        %{
          topic: "events",
          partitions: [
            %{partition: 0, timestamp: -1}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # isolation_level (1 = read_committed)
      1,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      6,
      "events"::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition (0)
      0,
      0,
      0,
      0,
      # timestamp (-1 = latest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.ListOffsets.V4.Request{
      correlation_id: 4,
      client_id: "test",
      replica_id: -1,
      isolation_level: 0,
      topics: [
        %{
          topic: "topic",
          partitions: [
            %{
              partition: 0,
              current_leader_epoch: 5,
              timestamp: -1
            }
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # isolation_level (0)
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
      # partition (0)
      0,
      0,
      0,
      0,
      # current_leader_epoch (5)
      0,
      0,
      0,
      5,
      # timestamp (-1 = latest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.ListOffsets.V5.Request{
      correlation_id: 5,
      client_id: "test",
      replica_id: -1,
      isolation_level: 1,
      topics: [
        %{
          topic: "data",
          partitions: [
            %{
              partition: 1,
              current_leader_epoch: 10,
              timestamp: -2
            }
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (2)
      0,
      2,
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
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # isolation_level (1 = read_committed)
      1,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name
      0,
      4,
      "data"::binary,
      # partitions array length (1)
      0,
      0,
      0,
      1,
      # partition (1)
      0,
      0,
      0,
      1,
      # current_leader_epoch (10)
      0,
      0,
      0,
      10,
      # timestamp (-2 = earliest)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      254
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
      iex> {actual, <<>>} = Kayrock.ListOffsets.V0.Response.deserialize(response_binary)
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      5,
      "topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      0,
      # error_code (0 = no error)
      0,
      0,
      # offsets array length (1)
      0,
      0,
      0,
      1,
      # offset (100)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100
    >>

    expected_struct = %Kayrock.ListOffsets.V0.Response{
      correlation_id: 0,
      responses: [
        %{
          topic: "topic",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              offsets: [100]
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      5,
      "topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      0,
      # error_code (0 = no error)
      0,
      0,
      # timestamp (42)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      42,
      # offset (100)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      100
    >>

    expected_struct = %Kayrock.ListOffsets.V1.Response{
      correlation_id: 1,
      responses: [
        %{
          topic: "topic",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              timestamp: 42,
              offset: 100
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
      # throttle_time_ms (10ms)
      0,
      0,
      0,
      10,
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      5,
      "topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      0,
      # error_code (0 = no error)
      0,
      0,
      # timestamp (1000)
      0,
      0,
      0,
      0,
      0,
      0,
      3,
      232,
      # offset (200)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      200
    >>

    expected_struct = %Kayrock.ListOffsets.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 10,
      responses: [
        %{
          topic: "topic",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              timestamp: 1000,
              offset: 200
            }
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
      # throttle_time_ms (0ms)
      0,
      0,
      0,
      0,
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "events"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      0,
      # error_code (0 = no error)
      0,
      0,
      # timestamp (5000)
      0,
      0,
      0,
      0,
      0,
      0,
      19,
      136,
      # offset (500)
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      244
    >>

    expected_struct = %Kayrock.ListOffsets.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "events",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              timestamp: 5000,
              offset: 500
            }
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
      # throttle_time_ms (5ms)
      0,
      0,
      0,
      5,
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      5,
      "topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      0,
      # error_code (0 = no error)
      0,
      0,
      # timestamp (2000)
      0,
      0,
      0,
      0,
      0,
      0,
      7,
      208,
      # offset (300)
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      44,
      # leader_epoch (5)
      0,
      0,
      0,
      5
    >>

    expected_struct = %Kayrock.ListOffsets.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 5,
      responses: [
        %{
          topic: "topic",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              timestamp: 2000,
              offset: 300,
              leader_epoch: 5
            }
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic
      0,
      4,
      "data"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition
      0,
      0,
      0,
      1,
      # error_code (0 = no error)
      0,
      0,
      # timestamp (3000)
      0,
      0,
      0,
      0,
      0,
      0,
      11,
      184,
      # offset (400)
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      144,
      # leader_epoch (10)
      0,
      0,
      0,
      10
    >>

    expected_struct = %Kayrock.ListOffsets.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "data",
          partition_responses: [
            %{
              partition: 1,
              error_code: 0,
              timestamp: 3000,
              offset: 400,
              leader_epoch: 10
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions for Custom Scenarios
  # ============================================

  @doc """
  Builds a custom response binary for V0-V5 with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:topic` - Default: "topic"
  - `:partition` - Default: 0
  - `:offset` - Default: 100
  - `:timestamp` - V1+ (default: 42)
  - `:throttle_time_ms` - V2+ (default: 0)
  - `:leader_epoch` - V4+ (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    offset = Keyword.get(opts, :offset, 100)

    <<
      correlation_id::32,
      # responses array length (1)
      1::32,
      byte_size(topic)::16,
      topic::binary,
      # partition_responses array length (1)
      1::32,
      partition::32,
      error_code::16-signed,
      # offsets array length (1)
      1::32,
      offset::64
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    topic = Keyword.get(opts, :topic, "topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    timestamp = Keyword.get(opts, :timestamp, 42)
    offset = Keyword.get(opts, :offset, 100)

    <<
      correlation_id::32,
      # responses array length (1)
      1::32,
      byte_size(topic)::16,
      topic::binary,
      # partition_responses array length (1)
      1::32,
      partition::32,
      error_code::16-signed,
      timestamp::64,
      offset::64
    >>
  end

  def response_binary(version, opts) when version in [2, 3] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    timestamp = Keyword.get(opts, :timestamp, 1000)
    offset = Keyword.get(opts, :offset, 100)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # responses array length (1)
      1::32,
      byte_size(topic)::16,
      topic::binary,
      # partition_responses array length (1)
      1::32,
      partition::32,
      error_code::16-signed,
      timestamp::64,
      offset::64
    >>
  end

  def response_binary(version, opts) when version in [4, 5] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    topic = Keyword.get(opts, :topic, "topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    timestamp = Keyword.get(opts, :timestamp, 2000)
    offset = Keyword.get(opts, :offset, 100)
    leader_epoch = Keyword.get(opts, :leader_epoch, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # responses array length (1)
      1::32,
      byte_size(topic)::16,
      topic::binary,
      # partition_responses array length (1)
      1::32,
      partition::32,
      error_code::16-signed,
      timestamp::64,
      offset::64,
      leader_epoch::32
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 1 (OFFSET_OUT_OF_RANGE)
  - All other options passed to response_binary/2
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 1)
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
