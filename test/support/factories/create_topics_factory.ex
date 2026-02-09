defmodule Kayrock.Test.Factories.CreateTopicsFactory do
  @moduledoc """
  Factory for generating CreateTopics API test data (V0-V5).

  API Key: 19
  Used to: Create new topics on the Kafka cluster.

  Protocol changes by version:
  - V0: Basic topic creation (topics, timeout_ms)
  - V1+: Adds validate_only flag and error_message in response
  - V2+: Adds throttle_time_ms in response
  - V5: Flexible/compact format with tagged_fields, num_partitions,
        replication_factor, and configs in response
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
    request = %Kayrock.CreateTopics.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topics: [
        %{
          name: "new-topic",
          num_partitions: 3,
          replication_factor: 1,
          assignments: [],
          configs: []
        }
      ],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key (19 = CreateTopics)
      0,
      19,
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
      "test"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      9,
      # topic name
      "new-topic"::binary,
      # num_partitions (3)
      0,
      0,
      0,
      3,
      # replication_factor (1)
      0,
      1,
      # assignments array length (0)
      0,
      0,
      0,
      0,
      # configs array length (0)
      0,
      0,
      0,
      0,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.CreateTopics.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topics: [
        %{
          name: "topic-1",
          num_partitions: 1,
          replication_factor: 1,
          assignments: [],
          configs: []
        }
      ],
      timeout_ms: 30_000,
      validate_only: false
    }

    expected_binary = <<
      # api_key (19)
      0,
      19,
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
      "test"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      7,
      # topic name
      "topic-1"::binary,
      # num_partitions (1)
      0,
      0,
      0,
      1,
      # replication_factor (1)
      0,
      1,
      # assignments array length (0)
      0,
      0,
      0,
      0,
      # configs array length (0)
      0,
      0,
      0,
      0,
      # timeout_ms (30000)
      0,
      0,
      117,
      48,
      # validate_only (false)
      0
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.CreateTopics.V2.Request{
      correlation_id: 2,
      client_id: "test",
      topics: [],
      timeout_ms: 60_000,
      validate_only: true
    }

    expected_binary = <<
      # api_key (19)
      0,
      19,
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
      "test"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # timeout_ms (60000)
      0,
      0,
      234,
      96,
      # validate_only (true)
      1
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.CreateTopics.V3.Request{
      correlation_id: 3,
      client_id: "test",
      topics: [
        %{
          name: "test-topic",
          num_partitions: 2,
          replication_factor: 2,
          assignments: [],
          configs: []
        }
      ],
      timeout_ms: 30_000,
      validate_only: false
    }

    expected_binary = <<
      # api_key (19)
      0,
      19,
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
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      10,
      # topic name
      "test-topic"::binary,
      # num_partitions (2)
      0,
      0,
      0,
      2,
      # replication_factor (2)
      0,
      2,
      # assignments array length (0)
      0,
      0,
      0,
      0,
      # configs array length (0)
      0,
      0,
      0,
      0,
      # timeout_ms (30000)
      0,
      0,
      117,
      48,
      # validate_only (false)
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.CreateTopics.V4.Request{
      correlation_id: 4,
      client_id: "test",
      topics: [
        %{
          name: "topic",
          num_partitions: 1,
          replication_factor: 1,
          assignments: [],
          configs: [
            %{name: "retention.ms", value: "86400000"}
          ]
        }
      ],
      timeout_ms: 30_000,
      validate_only: false
    }

    expected_binary = <<
      # api_key (19)
      0,
      19,
      # api_version (4)
      0,
      4,
      # correlation_id
      0,
      0,
      0,
      4,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # num_partitions (1)
      0,
      0,
      0,
      1,
      # replication_factor (1)
      0,
      1,
      # assignments array length (0)
      0,
      0,
      0,
      0,
      # configs array length (1)
      0,
      0,
      0,
      1,
      # config name length
      0,
      12,
      # config name
      "retention.ms"::binary,
      # config value length
      0,
      8,
      # config value
      "86400000"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48,
      # validate_only (false)
      0
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.CreateTopics.V5.Request{
      correlation_id: 5,
      client_id: "test",
      topics: [
        %{
          name: "topic",
          num_partitions: 1,
          replication_factor: 1,
          assignments: [],
          configs: [],
          tagged_fields: []
        }
      ],
      timeout_ms: 30_000,
      validate_only: false,
      tagged_fields: []
    }

    # V5 uses compact format
    expected_binary = <<
      # api_key (19)
      0,
      19,
      # api_version (5)
      0,
      5,
      # correlation_id
      0,
      0,
      0,
      5,
      # client_id length
      0,
      4,
      # client_id
      "test"::binary,
      # flexible header marker (tagged fields)
      0,
      # topics compact array (length + 1 = 2)
      2,
      # topic name compact string (length + 1 = 6)
      6,
      # topic name
      "topic"::binary,
      # num_partitions (1)
      0,
      0,
      0,
      1,
      # replication_factor (1)
      0,
      1,
      # assignments compact array (0 + 1 = 1 for empty)
      1,
      # configs compact array (0 + 1 = 1 for empty)
      1,
      # topic tagged_fields
      0,
      # timeout_ms (30000)
      0,
      0,
      117,
      48,
      # validate_only (false)
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
      iex> {actual, <<>>} = Kayrock.CreateTopics.V0.Response.deserialize(response_binary)
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
      # topics array length
      0,
      0,
      0,
      1,
      # topic name length
      0,
      9,
      # topic name
      "new-topic"::binary,
      # error_code (0 = no error)
      0,
      0
    >>

    expected_struct = %Kayrock.CreateTopics.V0.Response{
      correlation_id: 0,
      topics: [
        %{name: "new-topic", error_code: 0}
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
      # topics array length
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code
      0,
      0,
      # error_message (null = -1)
      255,
      255
    >>

    expected_struct = %Kayrock.CreateTopics.V1.Response{
      correlation_id: 1,
      topics: [
        %{name: "topic", error_code: 0, error_message: nil}
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
      100,
      # topics array length
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.CreateTopics.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      topics: [
        %{name: "topic", error_code: 0, error_message: nil}
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
      # topics array length
      0,
      0,
      0,
      1,
      # topic name length
      0,
      10,
      # topic name
      "test-topic"::binary,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.CreateTopics.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      topics: [
        %{name: "test-topic", error_code: 0, error_message: nil}
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
      50,
      # topics array length
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.CreateTopics.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 50,
      topics: [
        %{name: "topic", error_code: 0, error_message: nil}
      ]
    }

    {binary, expected_struct}
  end

  def response_data(5) do
    # V5 uses compact format with additional fields
    binary = <<
      # correlation_id
      0,
      0,
      0,
      5,
      # response header tagged_fields (varint: 0)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # topics compact array (length+1 = 2)
      2,
      # topic name compact string (length+1 = 6)
      6,
      # topic name
      "topic"::binary,
      # error_code
      0,
      0,
      # error_message compact nullable string (0 = null)
      0,
      # num_partitions (3)
      0,
      0,
      0,
      3,
      # replication_factor (1)
      0,
      1,
      # configs compact array (empty = 1)
      1,
      # topic tagged_fields
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.CreateTopics.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      topics: [
        %{
          name: "topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 3,
          replication_factor: 1,
          configs: [],
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
  Builds a custom response binary for V0-V4 with specified options.

  ## Options
  - `:correlation_id` - Default: based on version
  - `:topics` - List of topic results (default: single success topic)
  - `:throttle_time_ms` - V2+ only (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    topics = Keyword.get(opts, :topics, [%{name: "topic", error_code: 0}])

    topics_binary =
      for topic <- topics, into: <<>> do
        name = Map.get(topic, :name, "topic")
        error_code = Map.get(topic, :error_code, 0)
        <<byte_size(name)::16, name::binary, error_code::16-signed>>
      end

    <<
      correlation_id::32,
      length(topics)::32,
      topics_binary::binary
    >>
  end

  def response_binary(1, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    topics =
      Keyword.get(opts, :topics, [%{name: "topic", error_code: 0, error_message: nil}])

    topics_binary =
      for topic <- topics, into: <<>> do
        name = Map.get(topic, :name, "topic")
        error_code = Map.get(topic, :error_code, 0)
        error_message = Map.get(topic, :error_message, nil)

        error_msg_binary =
          case error_message do
            nil -> <<-1::16-signed>>
            msg -> <<byte_size(msg)::16, msg::binary>>
          end

        <<byte_size(name)::16, name::binary, error_code::16-signed, error_msg_binary::binary>>
      end

    <<
      correlation_id::32,
      length(topics)::32,
      topics_binary::binary
    >>
  end

  def response_binary(version, opts) when version in [2, 3, 4] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    topics =
      Keyword.get(opts, :topics, [%{name: "topic", error_code: 0, error_message: nil}])

    topics_binary =
      for topic <- topics, into: <<>> do
        name = Map.get(topic, :name, "topic")
        error_code = Map.get(topic, :error_code, 0)
        error_message = Map.get(topic, :error_message, nil)

        error_msg_binary =
          case error_message do
            nil -> <<-1::16-signed>>
            msg -> <<byte_size(msg)::16, msg::binary>>
          end

        <<byte_size(name)::16, name::binary, error_code::16-signed, error_msg_binary::binary>>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      length(topics)::32,
      topics_binary::binary
    >>
  end

  def response_binary(5, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 5)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)

    topics =
      Keyword.get(opts, :topics, [
        %{
          name: "topic",
          error_code: 0,
          error_message: nil,
          num_partitions: 1,
          replication_factor: 1
        }
      ])

    topics_binary =
      for topic <- topics, into: <<>> do
        name = Map.get(topic, :name, "topic")
        error_code = Map.get(topic, :error_code, 0)
        error_message = Map.get(topic, :error_message, nil)
        num_partitions = Map.get(topic, :num_partitions, 1)
        replication_factor = Map.get(topic, :replication_factor, 1)

        # Compact string: length+1, 0 for null
        error_msg_binary =
          case error_message do
            nil -> <<0>>
            msg -> <<byte_size(msg) + 1, msg::binary>>
          end

        # Compact string: length+1
        name_binary = <<byte_size(name) + 1, name::binary>>

        <<
          name_binary::binary,
          error_code::16-signed,
          error_msg_binary::binary,
          num_partitions::32-signed,
          replication_factor::16-signed,
          # configs compact array (empty = 1)
          1,
          # topic tagged_fields
          0
        >>
      end

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      # topics compact array: length+1
      length(topics) + 1,
      topics_binary::binary,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds a minimal valid response (empty topics).
  """
  def minimal_response(version, opts \\ []) do
    response_binary(version, Keyword.put(opts, :topics, []))
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 36 (TOPIC_ALREADY_EXISTS)
  - `:topic_name` - Default: "topic"
  - `:error_message` - Error message (V1+ only, default: nil)
  """
  def error_response(version, opts \\ []) do
    topic_name = Keyword.get(opts, :topic_name, "topic")
    error_code = Keyword.get(opts, :error_code, 36)
    error_message = Keyword.get(opts, :error_message, nil)

    topic =
      if version >= 5 do
        %{
          name: topic_name,
          error_code: error_code,
          error_message: error_message,
          num_partitions: -1,
          replication_factor: -1
        }
      else
        %{
          name: topic_name,
          error_code: error_code,
          error_message: error_message
        }
      end

    response_binary(version, Keyword.put(opts, :topics, [topic]))
  end

  @doc """
  Builds a response with multiple topics.
  """
  def multi_topic_response(version, opts \\ []) do
    topics =
      if version >= 5 do
        [
          %{
            name: "topic-1",
            error_code: 0,
            error_message: nil,
            num_partitions: 3,
            replication_factor: 1
          },
          %{
            name: "topic-2",
            error_code: 36,
            error_message: nil,
            num_partitions: -1,
            replication_factor: -1
          }
        ]
      else
        [
          %{name: "topic-1", error_code: 0, error_message: nil},
          %{name: "topic-2", error_code: 36, error_message: nil}
        ]
      end

    response_binary(version, Keyword.put(opts, :topics, topics))
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
