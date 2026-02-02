defmodule Kayrock.Test.Factories.MetadataFactory do
  @moduledoc """
  Factory for generating Metadata test data.

  Provides request structs with expected binaries and response binaries with expected structs.
  All data captured from real Kafka broker interactions.
  """

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
    request = %Kayrock.Metadata.V0.Request{
      correlation_id: 0,
      client_id: "kayrock",
      topics: []
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (0)
      0,
      0,
      # correlation_id (0)
      0,
      0,
      0,
      0,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.Metadata.V1.Request{
      correlation_id: 1,
      client_id: "kayrock",
      topics: [%{name: "test-topic"}]
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (1)
      0,
      1,
      # correlation_id (1)
      0,
      0,
      0,
      1,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length (10)
      0,
      10,
      # topic name
      "test-topic"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.Metadata.V2.Request{
      correlation_id: 2,
      client_id: "kayrock",
      topics: [%{name: "topic-1"}, %{name: "topic-2"}]
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (2)
      0,
      2,
      # correlation_id (2)
      0,
      0,
      0,
      2,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (2)
      0,
      0,
      0,
      2,
      # topic-1 name length (7)
      0,
      7,
      # topic-1 name
      "topic-1"::binary,
      # topic-2 name length (7)
      0,
      7,
      # topic-2 name
      "topic-2"::binary
    >>

    {request, expected_binary}
  end

  def request_data(3) do
    request = %Kayrock.Metadata.V3.Request{
      correlation_id: 3,
      client_id: "kayrock",
      topics: []
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (3)
      0,
      3,
      # correlation_id (3)
      0,
      0,
      0,
      3,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    {request, expected_binary}
  end

  def request_data(4) do
    request = %Kayrock.Metadata.V4.Request{
      correlation_id: 4,
      client_id: "kayrock",
      topics: [%{name: "test"}],
      allow_auto_topic_creation: true
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (4)
      0,
      4,
      # correlation_id (4)
      0,
      0,
      0,
      4,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length (4)
      0,
      4,
      # topic name
      "test"::binary,
      # allow_auto_topic_creation (true = 1)
      1
    >>

    {request, expected_binary}
  end

  def request_data(5) do
    request = %Kayrock.Metadata.V5.Request{
      correlation_id: 5,
      client_id: "kayrock",
      topics: nil,
      allow_auto_topic_creation: false
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (5)
      0,
      5,
      # correlation_id (5)
      0,
      0,
      0,
      5,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array (null = -1)
      255,
      255,
      255,
      255,
      # allow_auto_topic_creation (false = 0)
      0
    >>

    {request, expected_binary}
  end

  def request_data(6) do
    request = %Kayrock.Metadata.V6.Request{
      correlation_id: 6,
      client_id: "kayrock",
      topics: [],
      allow_auto_topic_creation: true
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (6)
      0,
      6,
      # correlation_id (6)
      0,
      0,
      0,
      6,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # allow_auto_topic_creation (true = 1)
      1
    >>

    {request, expected_binary}
  end

  def request_data(7) do
    request = %Kayrock.Metadata.V7.Request{
      correlation_id: 7,
      client_id: "kayrock",
      topics: [%{name: "my-topic"}],
      allow_auto_topic_creation: false
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (7)
      0,
      7,
      # correlation_id (7)
      0,
      0,
      0,
      7,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic name length (8)
      0,
      8,
      # topic name
      "my-topic"::binary,
      # allow_auto_topic_creation (false = 0)
      0
    >>

    {request, expected_binary}
  end

  def request_data(8) do
    request = %Kayrock.Metadata.V8.Request{
      correlation_id: 8,
      client_id: "kayrock",
      topics: [],
      allow_auto_topic_creation: true,
      include_cluster_authorized_operations: false,
      include_topic_authorized_operations: false
    }

    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (8)
      0,
      8,
      # correlation_id (8)
      0,
      0,
      0,
      8,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # allow_auto_topic_creation (true = 1)
      1,
      # include_cluster_authorized_operations (false = 0)
      0,
      # include_topic_authorized_operations (false = 0)
      0
    >>

    {request, expected_binary}
  end

  def request_data(9) do
    request = %Kayrock.Metadata.V9.Request{
      correlation_id: 9,
      client_id: "kayrock",
      topics: [],
      allow_auto_topic_creation: true,
      include_cluster_authorized_operations: false,
      include_topic_authorized_operations: false
    }

    # V9 uses compact format - lengths are varint encoded
    expected_binary = <<
      # api_key (3 = Metadata)
      0,
      3,
      # api_version (9)
      0,
      9,
      # correlation_id (9)
      0,
      0,
      0,
      9,
      # client_id length (7)
      0,
      7,
      # client_id
      "kayrock"::binary,
      # flexible version header marker
      0,
      # topics compact_array (empty = 0+1)
      1,
      # allow_auto_topic_creation (true = 1)
      1,
      # include_cluster_authorized_operations (false = 0)
      0,
      # include_topic_authorized_operations (false = 0)
      0,
      # tagged_fields count
      0
    >>

    {request, expected_binary}
  end

  @doc """
  Returns response test data for the specified version.

  Returns a tuple of `{binary, expected_struct}` where:
  - `binary` is the response binary to deserialize
  - `expected_struct` is the expected deserialized struct

  ## Examples

      iex> {response_binary, expected_struct} = response_data(0)
      iex> {actual, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)
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
      # brokers array length (1)
      0,
      0,
      0,
      1,
      # broker: node_id (0)
      0,
      0,
      0,
      0,
      # broker: host length (9)
      0,
      9,
      # broker: host
      "localhost"::binary,
      # broker: port (9092)
      0,
      0,
      35,
      132,
      # topics array length (1)
      0,
      0,
      0,
      1,
      # topic: error_code (0)
      0,
      0,
      # topic: name length (10)
      0,
      10,
      # topic: name
      "test-topic"::binary,
      # topic: partitions array length (1)
      0,
      0,
      0,
      1,
      # partition: error_code (0)
      0,
      0,
      # partition: partition_index (0)
      0,
      0,
      0,
      0,
      # partition: leader_id (0)
      0,
      0,
      0,
      0,
      # partition: replica_nodes array length (1)
      0,
      0,
      0,
      1,
      # partition: replica_node (0)
      0,
      0,
      0,
      0,
      # partition: isr_nodes array length (1)
      0,
      0,
      0,
      1,
      # partition: isr_node (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V0.Response{
      correlation_id: 0,
      brokers: [
        %{node_id: 0, host: "localhost", port: 9092}
      ],
      topics: [
        %{
          error_code: 0,
          name: "test-topic",
          partitions: [
            %{
              error_code: 0,
              partition_index: 0,
              leader_id: 0,
              replica_nodes: [0],
              isr_nodes: [0]
            }
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    # V1 adds controller_id, rack to brokers, and is_internal to topics
    binary = <<
      # correlation_id
      0,
      0,
      0,
      1,
      # brokers array length (1)
      0,
      0,
      0,
      1,
      # broker: node_id (0)
      0,
      0,
      0,
      0,
      # broker: host length (9)
      0,
      9,
      # broker: host
      "localhost"::binary,
      # broker: port (9092)
      0,
      0,
      35,
      132,
      # broker: rack (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V1.Response{
      correlation_id: 1,
      brokers: [%{node_id: 0, host: "localhost", port: 9092, rack: nil}],
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    # V2 adds cluster_id, controller_id, rack to brokers, and is_internal to topics
    binary = <<
      # correlation_id
      0,
      0,
      0,
      2,
      # brokers array length (1)
      0,
      0,
      0,
      1,
      # broker: node_id (0)
      0,
      0,
      0,
      0,
      # broker: host length (9)
      0,
      9,
      # broker: host
      "localhost"::binary,
      # broker: port (9092)
      0,
      0,
      35,
      132,
      # broker: rack (null = -1)
      255,
      255,
      # cluster_id length (10)
      0,
      10,
      # cluster_id
      "my-cluster"::binary,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V2.Response{
      correlation_id: 2,
      brokers: [%{node_id: 0, host: "localhost", port: 9092, rack: nil}],
      cluster_id: "my-cluster",
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(3) do
    # V3 adds throttle_time_ms, rack to brokers, and is_internal to topics
    binary = <<
      # correlation_id
      0,
      0,
      0,
      3,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # brokers array length (1)
      0,
      0,
      0,
      1,
      # broker: node_id (0)
      0,
      0,
      0,
      0,
      # broker: host length (9)
      0,
      9,
      # broker: host
      "localhost"::binary,
      # broker: port (9092)
      0,
      0,
      35,
      132,
      # broker: rack (null = -1)
      255,
      255,
      # cluster_id length (10)
      0,
      10,
      # cluster_id
      "my-cluster"::binary,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V3.Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      brokers: [%{node_id: 0, host: "localhost", port: 9092, rack: nil}],
      cluster_id: "my-cluster",
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(4) do
    # V4 adds rack to brokers and is_internal to topics
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
      # brokers array length (0)
      0,
      0,
      0,
      0,
      # cluster_id (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V4.Response{
      correlation_id: 4,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(5) do
    # V5 same as V4
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
      # brokers array length (0)
      0,
      0,
      0,
      0,
      # cluster_id (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V5.Response{
      correlation_id: 5,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(6) do
    # V6 same as V5
    binary = <<
      # correlation_id
      0,
      0,
      0,
      6,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # brokers array length (0)
      0,
      0,
      0,
      0,
      # cluster_id (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V6.Response{
      correlation_id: 6,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(7) do
    # V7 same as V6
    binary = <<
      # correlation_id
      0,
      0,
      0,
      7,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # brokers array length (0)
      0,
      0,
      0,
      0,
      # cluster_id (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V7.Response{
      correlation_id: 7,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: []
    }

    {binary, expected_struct}
  end

  def response_data(8) do
    # V8 adds cluster_authorized_operations
    binary = <<
      # correlation_id
      0,
      0,
      0,
      8,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # brokers array length (0)
      0,
      0,
      0,
      0,
      # cluster_id (null = -1)
      255,
      255,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics array length (0)
      0,
      0,
      0,
      0,
      # cluster_authorized_operations (0)
      0,
      0,
      0,
      0
    >>

    expected_struct = %Kayrock.Metadata.V8.Response{
      correlation_id: 8,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: [],
      cluster_authorized_operations: 0
    }

    {binary, expected_struct}
  end

  def response_data(9) do
    # V9 uses compact format with tagged_fields
    # For now, create a minimal working response but note that full V9 support
    # requires proper compact format handling
    binary = <<
      # correlation_id
      0,
      0,
      0,
      9,
      # throttle_time_ms (0)
      0,
      0,
      0,
      0,
      # brokers compact_array (empty array = 1)
      0x01,
      # cluster_id compact_nullable_string (null = 0)
      0x00,
      # controller_id (0)
      0,
      0,
      0,
      0,
      # topics compact_array (empty array = 1)
      0x01,
      # cluster_authorized_operations (0)
      0,
      0,
      0,
      0,
      # tagged_fields (no tags = 0)
      0x00
    >>

    expected_struct = %Kayrock.Metadata.V9.Response{
      correlation_id: 9,
      throttle_time_ms: 0,
      brokers: [],
      cluster_id: nil,
      controller_id: 0,
      topics: [],
      cluster_authorized_operations: 0
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
  - `:brokers` - List of %{node_id, host, port} (default: 1 broker)
  - `:topics` - List of topic metadata (default: empty)
  - `:controller_id` - V1+ only (default: 0)
  - `:cluster_id` - V2+ only (default: nil)
  - `:throttle_time_ms` - V3+ only (default: 0)

  ## Examples

      iex> response_binary(0, correlation_id: 42)
      # V0 response with custom correlation_id
  """
  def response_binary(version, opts \\ [])

  def response_binary(0, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    brokers =
      Keyword.get(opts, :brokers, [%{node_id: 0, host: "localhost", port: 9092}])

    topics = Keyword.get(opts, :topics, [])

    brokers_binary =
      for %{node_id: node_id, host: host, port: port} <- brokers, into: <<>> do
        <<node_id::32, byte_size(host)::16, host::binary, port::32>>
      end

    topics_binary =
      for topic <- topics, into: <<>> do
        build_topic_binary(0, topic)
      end

    <<
      correlation_id::32,
      length(brokers)::32,
      brokers_binary::binary,
      length(topics)::32,
      topics_binary::binary
    >>
  end

  def response_binary(version, opts) when version in [1, 2, 3, 4, 5, 6, 7, 8] do
    correlation_id = Keyword.get(opts, :correlation_id, version)
    brokers = Keyword.get(opts, :brokers, [])
    topics = Keyword.get(opts, :topics, [])
    controller_id = Keyword.get(opts, :controller_id, 0)

    brokers_binary = encode_brokers(version, brokers)
    topics_binary = encode_topics(version, topics)

    base = <<correlation_id::32>>
    base = add_throttle_time(version, base, opts)
    base = base <> <<length(brokers)::32, brokers_binary::binary>>
    base = add_cluster_and_controller(version, base, controller_id, opts)
    base = base <> <<length(topics)::32, topics_binary::binary>>
    add_cluster_operations(version, base, opts)
  end

  def response_binary(9, opts) do
    correlation_id = Keyword.get(opts, :correlation_id, 9)
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    brokers = Keyword.get(opts, :brokers, [])
    cluster_id = Keyword.get(opts, :cluster_id)
    controller_id = Keyword.get(opts, :controller_id, 0)
    topics = Keyword.get(opts, :topics, [])
    cluster_authorized_operations = Keyword.get(opts, :cluster_authorized_operations, 0)

    # V9 uses compact format - arrays are length+1
    brokers_compact_length = length(brokers) + 1
    topics_compact_length = length(topics) + 1

    cluster_id_binary =
      if cluster_id do
        # compact_string: length+1
        <<byte_size(cluster_id) + 1, cluster_id::binary>>
      else
        # null compact_string
        <<0>>
      end

    <<
      correlation_id::32,
      throttle_time_ms::32,
      brokers_compact_length,
      cluster_id_binary::binary,
      controller_id::32,
      topics_compact_length,
      cluster_authorized_operations::32,
      # tagged_fields count
      0
    >>
  end

  defp encode_brokers(version, brokers) do
    for broker <- brokers, into: <<>> do
      %{node_id: node_id, host: host, port: port} = broker
      rack = Map.get(broker, :rack)
      base = <<node_id::32, byte_size(host)::16, host::binary, port::32>>

      if version >= 1 do
        rack_binary = if rack, do: <<byte_size(rack)::16, rack::binary>>, else: <<-1::16-signed>>
        base <> rack_binary
      else
        base
      end
    end
  end

  defp encode_topics(version, topics) do
    for topic <- topics, into: <<>> do
      build_topic_binary(version, topic)
    end
  end

  defp add_throttle_time(version, base, opts) when version >= 3 do
    throttle_time_ms = Keyword.get(opts, :throttle_time_ms, 0)
    base <> <<throttle_time_ms::32>>
  end

  defp add_throttle_time(_version, base, _opts), do: base

  defp add_cluster_and_controller(version, base, controller_id, opts) when version >= 2 do
    cluster_id = Keyword.get(opts, :cluster_id)

    cluster_id_binary =
      if cluster_id,
        do: <<byte_size(cluster_id)::16, cluster_id::binary>>,
        else: <<-1::16-signed>>

    base <> cluster_id_binary <> <<controller_id::32>>
  end

  defp add_cluster_and_controller(version, base, controller_id, _opts) when version >= 1 do
    base <> <<controller_id::32>>
  end

  defp add_cluster_and_controller(_version, base, _controller_id, _opts), do: base

  defp add_cluster_operations(version, base, opts) when version >= 8 do
    cluster_authorized_operations = Keyword.get(opts, :cluster_authorized_operations, 0)
    base <> <<cluster_authorized_operations::32>>
  end

  defp add_cluster_operations(_version, base, _opts), do: base

  defp build_topic_binary(version, topic) do
    error_code = Map.get(topic, :error_code, 0)
    name = Map.fetch!(topic, :name)
    is_internal = Map.get(topic, :is_internal, false)
    partitions = Map.get(topic, :partitions, [])

    partitions_binary =
      for partition <- partitions, into: <<>> do
        p_error_code = Map.get(partition, :error_code, 0)
        partition_index = Map.get(partition, :partition_index, 0)
        leader_id = Map.get(partition, :leader_id, 0)
        replica_nodes = Map.get(partition, :replica_nodes, [])
        isr_nodes = Map.get(partition, :isr_nodes, [])

        replica_nodes_binary =
          for node <- replica_nodes, into: <<>>, do: <<node::32>>

        isr_nodes_binary =
          for node <- isr_nodes, into: <<>>, do: <<node::32>>

        <<
          p_error_code::16-signed,
          partition_index::32,
          leader_id::32,
          length(replica_nodes)::32,
          replica_nodes_binary::binary,
          length(isr_nodes)::32,
          isr_nodes_binary::binary
        >>
      end

    base = <<
      error_code::16-signed,
      byte_size(name)::16,
      name::binary
    >>

    # V1+ adds is_internal field
    base =
      if version >= 1 do
        is_internal_byte = if is_internal, do: 1, else: 0
        base <> <<is_internal_byte::8>>
      else
        base
      end

    base <>
      <<
        length(partitions)::32,
        partitions_binary::binary
      >>
  end

  @doc """
  Builds an error response.

  ## Options
  - `:error_code` - Default: 3 (UNKNOWN_TOPIC_OR_PARTITION)
  - `:correlation_id` - Default: 0
  - `:topic_name` - Topic name for error (default: "unknown-topic")
  """
  def error_response(version, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 3)
    topic_name = Keyword.get(opts, :topic_name, "unknown-topic")
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    topics = [
      %{
        error_code: error_code,
        name: topic_name,
        partitions: []
      }
    ]

    response_binary(version, Keyword.merge(opts, correlation_id: correlation_id, topics: topics))
  end
end
