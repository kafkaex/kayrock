defmodule Kayrock.Test.Factories.DeleteTopicsFactory do
  @moduledoc """
  Factory for generating DeleteTopics API test data (V0-V4).

  API Key: 20
  Used to: Delete topics from the Kafka cluster.

  Protocol changes by version:
  - V0: Basic topic deletion (responses only)
  - V1+: Adds throttle_time_ms in response
  - V4: Compact/flexible format with tagged_fields
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
    request = %Kayrock.DeleteTopics.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topic_names: ["topic1", "topic2"],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key (20 = DeleteTopics)
      0,
      20,
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
      # topic_names array length (2)
      0,
      0,
      0,
      2,
      # topic1 length
      0,
      6,
      # topic1
      "topic1"::binary,
      # topic2 length
      0,
      6,
      # topic2
      "topic2"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DeleteTopics.V1.Request{
      correlation_id: 1,
      client_id: "test",
      topic_names: ["topic"],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key (20)
      0,
      20,
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
      # topic_names array length (1)
      0,
      0,
      0,
      1,
      # topic length
      0,
      5,
      # topic
      "topic"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.DeleteTopics.V2.Request{
      correlation_id: 2,
      client_id: "test",
      topic_names: ["topic"],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key (20)
      0,
      20,
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
      # topic_names array length (1)
      0,
      0,
      0,
      1,
      # topic length
      0,
      5,
      # topic
      "topic"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.DeleteTopics.V3.Request{
      correlation_id: 3,
      client_id: "test",
      topic_names: ["topic"],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key (20)
      0,
      20,
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
      # topic_names array length (1)
      0,
      0,
      0,
      1,
      # topic length
      0,
      5,
      # topic
      "topic"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.DeleteTopics.V4.Request{
      correlation_id: 4,
      client_id: "test",
      topic_names: ["topic"],
      timeout_ms: 30_000,
      tagged_fields: []
    }

    # V4 uses compact format
    expected_binary = <<
      # api_key (20)
      0,
      20,
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
      # flexible header tagged_fields (0)
      0,
      # topic_names compact array (length+1 = 2)
      2,
      # topic compact string (length+1 = 6)
      6,
      # topic
      "topic"::binary,
      # timeout_ms (30000)
      0,
      0,
      117,
      48,
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
      # responses array length (2)
      0,
      0,
      0,
      2,
      # topic1 name length
      0,
      6,
      # topic1 name
      "topic1"::binary,
      # error_code (0)
      0,
      0,
      # topic2 name length
      0,
      6,
      # topic2 name
      "topic2"::binary,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteTopics.V0.Response{
      correlation_id: 0,
      responses: [
        %{name: "topic1", error_code: 0},
        %{name: "topic2", error_code: 0}
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteTopics.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      responses: [
        %{name: "topic", error_code: 0}
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
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteTopics.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 0,
      responses: [
        %{name: "topic", error_code: 0}
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
      # throttle_time_ms (100)
      0,
      0,
      0,
      100,
      # responses array length (1)
      0,
      0,
      0,
      1,
      # topic name length
      0,
      5,
      # topic name
      "topic"::binary,
      # error_code (0)
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteTopics.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 100,
      responses: [
        %{name: "topic", error_code: 0}
      ]
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    # V4 uses compact format
    binary = <<
      # correlation_id
      0,
      0,
      0,
      4,
      # response header tagged_fields
      0,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # responses compact array (length+1 = 2)
      2,
      # topic name compact string (length+1 = 6)
      6,
      # topic name
      "topic"::binary,
      # error_code (0)
      0,
      0,
      # topic tagged_fields
      0,
      # response tagged_fields
      0
    >>

    expected_struct = %Kayrock.DeleteTopics.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      responses: [
        %{name: "topic", error_code: 0, tagged_fields: []}
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
  - `:responses` - List of topic results (default: single success topic)
  - `:throttle_time_ms` - V1+ only (default: 0)
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    responses = Keyword.get(opts, :responses, [%{name: "topic", error_code: 0}])

    responses_binary =
      for resp <- responses, into: <<>> do
        name = Map.get(resp, :name, "topic")
        error_code = Map.get(resp, :error_code, 0)
        <<byte_size(name)::16, name::binary, error_code::16-signed>>
      end

    <<
      correlation_id::32,
      length(responses)::32,
      responses_binary::binary
    >>
  end

  def response_binary(version, opts) when version in [1, 2, 3] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    responses = Keyword.get(opts, :responses, [%{name: "topic", error_code: 0}])

    responses_binary =
      for resp <- responses, into: <<>> do
        name = Map.get(resp, :name, "topic")
        error_code = Map.get(resp, :error_code, 0)
        <<byte_size(name)::16, name::binary, error_code::16-signed>>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      length(responses)::32,
      responses_binary::binary
    >>
  end

  def response_binary(4, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 4)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    responses = Keyword.get(opts, :responses, [%{name: "topic", error_code: 0}])

    responses_binary =
      for resp <- responses, into: <<>> do
        name = Map.get(resp, :name, "topic")
        error_code = Map.get(resp, :error_code, 0)
        # Compact string: length+1
        <<byte_size(name) + 1, name::binary, error_code::16-signed, 0>>
      end

    <<
      correlation_id::32,
      # response header tagged_fields
      0,
      throttle_time_ms::32,
      # responses compact array: length+1
      length(responses) + 1,
      responses_binary::binary,
      # response tagged_fields
      0
    >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 3 (UNKNOWN_TOPIC_OR_PARTITION)
  - `:topic_name` - Default: "topic"
  """
  def error_response(version, opts \\ []) do
    topic_name = Keyword.get(opts, :topic_name, "topic")
    error_code = Keyword.get(opts, :error_code, 3)

    response_binary(
      version,
      Keyword.put(opts, :responses, [%{name: topic_name, error_code: error_code}])
    )
  end

  @doc """
  Builds a multi-topic response (one success, one error).
  """
  def multi_topic_response(version, opts \\ []) do
    responses = [
      %{name: "topic-success", error_code: 0},
      %{name: "topic-error", error_code: 3}
    ]

    response_binary(version, Keyword.put(opts, :responses, responses))
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
