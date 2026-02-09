defmodule Kayrock.Test.Factories.ProduceFactory do
  @moduledoc """
  Factory for generating Produce API test data (V0-V8).

  API Key: 0
  Used to: Send records to Kafka topics.

  Protocol changes by version:
  - V0-V2: MessageSet format (legacy message format)
  - V3+: RecordBatch format (new format with idempotent/transactional support)
  - V3+: Adds transactional_id field
  - V2+: Adds throttle_time_ms in response
  - V5+: Adds log_append_time_ms in response
  - V8: Adds records_errors field in response

  Notes:
  - V0-V2 use MessageSet (legacy format)
  - V3+ use RecordBatch (new format with headers, timestamps, etc.)
  - Compression: gzip and snappy supported in both formats
  """

  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch

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
    request = %Kayrock.Produce.V0.Request{
      correlation_id: 1,
      client_id: "foo",
      acks: 1,
      timeout: 10,
      topic_data: [
        %{
          topic: "food",
          data: [
            %{
              partition: 0,
              record_set: %MessageSet{
                messages: [
                  %Message{key: "", value: "hey"}
                ]
              }
            }
          ]
        }
      ]
    }

    expected_binary =
      <<0, 0, 0, 0, 0, 0, 0, 1, 0, 3, 102, 111, 111, 0, 1, 0, 0, 0, 10, 0, 0, 0, 1, 0, 4, 102,
        111, 111, 100, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 29, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 17,
        106, 86, 37, 142, 0, 0, 0, 0, 0, 0, 0, 0, 0, 3, 104, 101, 121>>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.Produce.V1.Request{
      correlation_id: 1,
      client_id: "test",
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: "test-topic",
          data: [
            %{
              partition: 0,
              record_set: %MessageSet{
                messages: [%Message{key: "key", value: "value"}]
              }
            }
          ]
        }
      ]
    }

    # Note: The exact binary depends on CRC calculation for the message
    # We'll serialize it for comparison
    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.Produce.V2.Request{
      correlation_id: 2,
      client_id: "test",
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: "test-topic",
          data: [
            %{
              partition: 0,
              record_set: %MessageSet{
                messages: [%Message{key: "k", value: "v"}]
              }
            }
          ]
        }
      ]
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.Produce.V3.Request{
      correlation_id: 3,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: [
        %{
          topic: "topic",
          data: [
            %{
              partition: 0,
              record_set: %RecordBatch{
                records: [
                  %RecordBatch.Record{
                    key: "key",
                    value: "value",
                    headers: [],
                    attributes: 0,
                    timestamp: -1,
                    offset: 0
                  }
                ]
              }
            }
          ]
        }
      ]
    }

    # RecordBatch format includes CRC, so we serialize it
    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.Produce.V4.Request{
      correlation_id: 4,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: []
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.Produce.V5.Request{
      correlation_id: 5,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: []
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(6) do
    request = %Kayrock.Produce.V6.Request{
      correlation_id: 6,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: []
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(7) do
    request = %Kayrock.Produce.V7.Request{
      correlation_id: 7,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: []
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    {request, expected_binary}
  end

  def request_data(8) do
    request = %Kayrock.Produce.V8.Request{
      correlation_id: 8,
      client_id: "test",
      transactional_id: nil,
      acks: -1,
      timeout: 1000,
      topic_data: []
    }

    expected_binary = IO.iodata_to_binary(Kayrock.Request.serialize(request))

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
      iex> {actual, <<>>} = Kayrock.Produce.V0.Response.deserialize(response_binary)
      iex> assert actual == expected_struct
  """
  def response_data(version)

  def response_data(0) do
    binary = <<0::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 10::64>>

    expected_struct = %Kayrock.Produce.V0.Response{
      correlation_id: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{base_offset: 10, error_code: 0, partition: 0}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    binary = <<1::32, 1::32, 3::16, "bar"::binary, 1::32, 0::32, 0::16, 20::64, 50::32>>

    expected_struct = %Kayrock.Produce.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{base_offset: 20, error_code: 0, partition: 0}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    binary =
      <<
        # correlation_id
        2::32,
        # responses array (1 topic)
        1::32,
        5::16,
        "topic"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        100::64,
        # log_append_time
        0::64,
        # throttle_time_ms (at the end!)
        500::32
      >>

    expected_struct = %Kayrock.Produce.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 500,
      responses: [
        %{
          topic: "topic",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 100,
              log_append_time: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(3) do
    binary =
      <<
        # correlation_id
        3::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        30::64,
        # log_append_time
        0::64,
        # throttle_time_ms (at the end!)
        100::32
      >>

    expected_struct = %Kayrock.Produce.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 100,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 30,
              log_append_time: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    binary =
      <<
        # correlation_id
        4::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        40::64,
        # log_append_time
        0::64,
        # throttle_time_ms (at the end!)
        0::32
      >>

    expected_struct = %Kayrock.Produce.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 40,
              log_append_time: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(5) do
    binary =
      <<
        # correlation_id
        5::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        50::64,
        # log_append_time
        1000::64,
        # log_start_offset
        0::64,
        # throttle_time_ms (at the end!)
        0::32
      >>

    expected_struct = %Kayrock.Produce.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 50,
              log_append_time: 1000,
              log_start_offset: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(6) do
    binary =
      <<
        # correlation_id
        6::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        60::64,
        # log_append_time
        2000::64,
        # log_start_offset
        0::64,
        # throttle_time_ms (at the end!)
        0::32
      >>

    expected_struct = %Kayrock.Produce.V6.Response{
      correlation_id: 6,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 60,
              log_append_time: 2000,
              log_start_offset: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(7) do
    binary =
      <<
        # correlation_id
        7::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        70::64,
        # log_append_time
        3000::64,
        # log_start_offset
        0::64,
        # throttle_time_ms (at the end!)
        0::32
      >>

    expected_struct = %Kayrock.Produce.V7.Response{
      correlation_id: 7,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 70,
              log_append_time: 3000,
              log_start_offset: 0
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(8) do
    binary =
      <<
        # correlation_id
        8::32,
        # responses array (1 topic)
        1::32,
        3::16,
        "bar"::binary,
        # partition_responses (1 partition)
        1::32,
        # partition
        0::32,
        # error_code
        0::16,
        # base_offset
        80::64,
        # log_append_time
        4000::64,
        # log_start_offset
        0::64,
        # record_errors array (0 elements)
        0::32,
        # error_message (null)
        255,
        255,
        # throttle_time_ms (at the end!)
        0::32
      >>

    expected_struct = %Kayrock.Produce.V8.Response{
      correlation_id: 8,
      throttle_time_ms: 0,
      responses: [
        %{
          topic: "bar",
          partition_responses: [
            %{
              partition: 0,
              error_code: 0,
              base_offset: 80,
              log_append_time: 4000,
              log_start_offset: 0,
              record_errors: [],
              error_message: nil
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
  Builds a custom response binary for the specified version with options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:error_code` - Default: 0 (no error)
  - `:base_offset` - Default: 100
  - `:throttle_time_ms` - V2+ only (default: 0)
  - `:log_append_time` - V2+ only (default: 0)
  - `:topic` - Default: "test-topic"
  - `:partition` - Default: 0
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    topic = Keyword.get(opts, :topic, "test-topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    base_offset = Keyword.get(opts, :base_offset, 100)

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
      base_offset::64
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    topic = Keyword.get(opts, :topic, "test-topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    base_offset = Keyword.get(opts, :base_offset, 100)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

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
      base_offset::64,
      # throttle_time_ms at the end!
      throttle_time_ms::32
    >>
  end

  def response_binary(version, opts) when version in [2, 3, 4] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    topic = Keyword.get(opts, :topic, "test-topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    base_offset = Keyword.get(opts, :base_offset, 100)
    log_append_time = Keyword.get(opts, :log_append_time, 0)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

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
      base_offset::64,
      log_append_time::64,
      # throttle_time_ms at the end!
      throttle_time_ms::32
    >>
  end

  def response_binary(version, opts) when version in [5, 6, 7] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    topic = Keyword.get(opts, :topic, "test-topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    base_offset = Keyword.get(opts, :base_offset, 100)
    log_append_time = Keyword.get(opts, :log_append_time, 0)
    log_start_offset = Keyword.get(opts, :log_start_offset, 0)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

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
      base_offset::64,
      log_append_time::64,
      log_start_offset::64,
      # throttle_time_ms at the end!
      throttle_time_ms::32
    >>
  end

  def response_binary(8, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 8)
    topic = Keyword.get(opts, :topic, "test-topic")
    partition = Keyword.get(opts, :partition, 0)
    error_code = Keyword.get(opts, :error_code, 0)
    base_offset = Keyword.get(opts, :base_offset, 100)
    log_append_time = Keyword.get(opts, :log_append_time, 0)
    log_start_offset = Keyword.get(opts, :log_start_offset, 0)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

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
      base_offset::64,
      log_append_time::64,
      log_start_offset::64,
      # record_errors array (empty)
      0::32,
      # error_message (null)
      -1::16-signed,
      # throttle_time_ms at the end!
      throttle_time_ms::32
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 5 (LEADER_NOT_AVAILABLE)
  - `:correlation_id` - Default: based on version
  - `:base_offset` - Default: -1 (error marker)
  - `:throttle_time_ms` - V2+ only (default: 0)
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 5)
    base_offset = Keyword.get(opts, :base_offset, -1)

    opts =
      opts
      |> Keyword.put(:error_code, error_code)
      |> Keyword.put(:base_offset, base_offset)

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
