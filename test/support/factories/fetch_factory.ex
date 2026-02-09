defmodule Kayrock.Test.Factories.FetchFactory do
  @moduledoc """
  Factory for generating Fetch test data.

  Provides request structs with expected binaries and response binaries with expected structs.
  All data captured from real Kafka 7.4.0 broker.

  Note: Fetch is one of the most complex APIs with:
  - V0-V3: MessageSet format (legacy)
  - V4+: RecordBatch format
  - V3+: max_bytes field
  - V4+: isolation_level field
  - V7+: session_id and session_epoch fields
  - V11+: rack_id field
  """

  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message

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
    request = %Kayrock.Fetch.V0.Request{
      correlation_id: 0,
      client_id: "kayrock-capture",
      replica_id: -1,
      max_wait_time: 100,
      min_bytes: 1,
      topics: [
        %{
          topic: "test-topic",
          partitions: [
            %{partition: 0, fetch_offset: 0, partition_max_bytes: 1_048_576}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (1 = Fetch)
      0,
      1,
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
      "kayrock-capture"::binary,
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # max_wait_time (100)
      0,
      0,
      0,
      100,
      # min_bytes (1)
      0,
      0,
      0,
      1,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic length (10)
      0,
      10,
      # topic name
      "test-topic"::binary,
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
      # fetch_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # partition_max_bytes (1048576)
      0,
      16,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.Fetch.V4.Request{
      correlation_id: 4,
      client_id: "kayrock-capture",
      replica_id: -1,
      max_wait_time: 100,
      min_bytes: 1,
      max_bytes: 10_000_000,
      isolation_level: 0,
      topics: [
        %{
          topic: "test-topic",
          partitions: [
            %{partition: 0, fetch_offset: 0, partition_max_bytes: 1_048_576}
          ]
        }
      ]
    }

    expected_binary = <<
      # api_key (1 = Fetch)
      0,
      1,
      # api_version (4)
      0,
      4,
      # correlation_id (4)
      0,
      0,
      0,
      4,
      # client_id length (15)
      0,
      15,
      # client_id
      "kayrock-capture"::binary,
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # max_wait_time (100)
      0,
      0,
      0,
      100,
      # min_bytes (1)
      0,
      0,
      0,
      1,
      # max_bytes (10000000)
      0,
      152,
      150,
      128,
      # isolation_level (0)
      0,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic length (10)
      0,
      10,
      # topic name
      "test-topic"::binary,
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
      # fetch_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # partition_max_bytes (1048576)
      0,
      16,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(11) do
    request = %Kayrock.Fetch.V11.Request{
      correlation_id: 11,
      client_id: "kayrock-capture",
      replica_id: -1,
      max_wait_time: 100,
      min_bytes: 1,
      max_bytes: 10_000_000,
      isolation_level: 0,
      session_id: 0,
      session_epoch: -1,
      topics: [
        %{
          topic: "test-topic",
          partitions: [
            %{
              partition: 0,
              current_leader_epoch: -1,
              fetch_offset: 0,
              log_start_offset: -1,
              partition_max_bytes: 1_048_576
            }
          ]
        }
      ],
      forgotten_topics_data: [],
      rack_id: ""
    }

    expected_binary = <<
      # api_key (1 = Fetch)
      0,
      1,
      # api_version (11)
      0,
      11,
      # correlation_id (11)
      0,
      0,
      0,
      11,
      # client_id length (15)
      0,
      15,
      # client_id
      "kayrock-capture"::binary,
      # flexible header marker
      0,
      # replica_id (-1)
      255,
      255,
      255,
      255,
      # max_wait_time (100)
      0,
      0,
      0,
      100,
      # min_bytes (1)
      0,
      0,
      0,
      1,
      # max_bytes (10000000)
      0,
      152,
      150,
      128,
      # isolation_level (0)
      0,
      # session_id (0)
      0,
      0,
      0,
      0,
      # session_epoch (-1)
      255,
      255,
      255,
      255,
      # topics array length (compact: 1+1)
      2,
      # topic length (compact: 10+1)
      11,
      # topic name
      "test-topic"::binary,
      # partitions array length (compact: 1+1)
      2,
      # partition (0)
      0,
      0,
      0,
      0,
      # current_leader_epoch (-1)
      255,
      255,
      255,
      255,
      # fetch_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # log_start_offset (-1)
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      255,
      # partition_max_bytes (1048576)
      0,
      16,
      0,
      0,
      # tagged_fields count
      0,
      # tagged_fields count
      0,
      # forgotten_topics_data array length (compact: 0+1)
      1,
      # tagged_fields count
      0,
      # rack_id (compact empty string: 0+1)
      1,
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
      iex> {actual, <<>>} = Kayrock.Fetch.V0.Response.deserialize(response_binary)
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
      # topic length (10)
      0,
      10,
      # topic name
      "test-topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0,
      # high_watermark (1)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      # message_set_size (29)
      0,
      0,
      0,
      29,
      # offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # message_size (17)
      0,
      0,
      0,
      17,
      # crc (0)
      0,
      0,
      0,
      0,
      # magic (0)
      0,
      # attributes (0)
      0,
      # key length (-1 = null)
      255,
      255,
      255,
      255,
      # value length (3)
      0,
      0,
      0,
      3,
      # value
      "foo"::binary
    >>

    expected_struct = %Kayrock.Fetch.V0.Response{
      correlation_id: 0,
      responses: [
        %{
          topic: "test-topic",
          partition_responses: [
            %{
              partition_header: %{
                partition: 0,
                error_code: 0,
                high_watermark: 1
              },
              record_set: %MessageSet{
                messages: [
                  %Message{
                    offset: 0,
                    crc: 0,
                    attributes: 0,
                    key: nil,
                    value: "foo"
                  }
                ]
              }
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    # V4+ uses RecordBatch format instead of MessageSet
    # This is a minimal response with no records
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic length (10)
      0,
      10,
      # topic name
      "test-topic"::binary,
      # partition_responses array length (1)
      0,
      0,
      0,
      1,
      # partition (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0,
      # high_watermark (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # last_stable_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # aborted_transactions array length (0)
      0,
      0,
      0,
      0,
      # record_set_size (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Fetch.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "test-topic",
          partition_responses: [
            %{
              partition_header: %{
                partition: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                aborted_transactions: []
              },
              record_set: nil
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(11) do
    # V11 with compact arrays (flexible version)
    binary = <<
      # correlation_id
      0,
      0,
      0,
      11,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # responses array length (compact: 1+1)
      2,
      # topic length (compact: 10+1)
      11,
      # topic name
      "test-topic"::binary,
      # partition_responses array length (compact: 1+1)
      2,
      # partition (0)
      0,
      0,
      0,
      0,
      # error_code (0)
      0,
      0,
      # high_watermark (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # last_stable_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # log_start_offset (0)
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      # aborted_transactions array length (compact: 0+1)
      1,
      # preferred_read_replica (-1)
      255,
      255,
      255,
      255,
      # tagged_fields count
      0,
      # record_set_size (0)
      0,
      0,
      0,
      0,
      # tagged_fields count
      0,
      # tagged_fields count
      0
    >>

    expected_struct = %Kayrock.Fetch.V11.Response{
      correlation_id: 11,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "test-topic",
          partition_responses: [
            %{
              partition_header: %{
                partition: 0,
                error_code: 0,
                high_watermark: 0,
                last_stable_offset: 0,
                log_start_offset: 0,
                aborted_transactions: [],
                preferred_read_replica: -1
              },
              record_set: nil
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
  Builds a custom response binary with specified options.

  ## Options
  - `:correlation_id` - Default: 0
  - `:throttle_time_ms` - V1+ only (default: 0)
  - `:error_code` - Error code for partition response (default: 0)
  - `:high_watermark` - Default: 0

  ## Examples

      iex> response_binary(0, correlation_id: 5, error_code: 1)
      # V0 response with error
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    high_watermark = Keyword.get(opts, :high_watermark, 0)

    <<
      correlation_id::32,
      # empty responses array
      0::32,
      # If error_code provided, add a single topic response
      if(error_code != 0,
        do: <<
          1::32,
          10::16,
          "test-topic"::binary,
          1::32,
          0::32,
          error_code::16,
          high_watermark::64,
          0::32
        >>,
        else: <<>>
      )::binary
    >>
  end

  def response_binary(4, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 4)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    error_code = Keyword.get(opts, :error_code, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # empty responses array
      0::32,
      # If error_code provided, add a single topic response
      if(error_code != 0,
        do: <<
          1::32,
          10::16,
          "test-topic"::binary,
          1::32,
          0::32,
          error_code::16,
          0::64,
          0::64,
          0::32,
          0::32
        >>,
        else: <<>>
      )::binary
    >>
  end

  def response_binary(11, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 11)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    <<
      correlation_id::32,
      throttle_time_ms::32,
      # empty responses array (compact: 0+1)
      1,
      # tagged_fields count
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 3 (UNKNOWN_TOPIC_OR_PARTITION)
  - `:correlation_id` - Default: 0
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    opts
    |> Keyword.put_new(:error_code, 3)
    |> then(&response_binary(version, &1))
  end
end
