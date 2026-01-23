defmodule Kayrock do
  @moduledoc """
  Idiomatic Elixir interface to the Kafka protocol.

  Kayrock provides serialization and deserialization for all Kafka protocol
  messages. It generates Elixir structs from the Kafka protocol schema,
  allowing you to work with Kafka at the wire protocol level.

  ## Core Features

  - Generated structs for every Kafka API version
  - Serialization to wire protocol (iodata)
  - Deserialization from binary responses
  - Compression support (gzip, snappy, LZ4, Zstandard)
  """

  alias Kayrock.BrokerConnection
  alias Kayrock.Client
  alias Kayrock.Request

  @typedoc """
  A `pid` for a `Kayrock.BrokerConnection` process.

  This is for low-level communication with individual brokers.  Generally you
  should work with a `client_pid`.
  """
  @type broker_pid :: pid

  @typedoc """
  A `pid` for a `Kayrock.Client` process.
  """
  @type client_pid :: pid

  @typedoc """
  Specifies the version of an API message

  Kayrock will generally try to pick a reasonable default here, but you can
  override it in many cases.  See [Kafka Protocol Documentation](https://kafka.apache.org/protocol.html#The_Messages_Metadata)
  """
  @type api_version :: non_neg_integer

  @typedoc """
  A broker's advertised integer node id
  """
  @type node_id :: integer

  @type topic_name :: binary

  @type partition_id :: non_neg_integer

  @typedoc """
  A node selector for sending messages to the cluster

  Generally the Kayrock API will select the right node depending on the
  operation.  These are exposed to provide fine-grained user control in a few
  cases where it might make sense to override the default.

  Possible values:
  * Integer - Directly access a node by id.  Generally you should not do this
    unless you are operating at a low level.
  * `:random` - Select a node at random from the cluster metadata.
  * `:controller` - Select the controller node - this is used for cluster
    management messages
  """
  @type node_selector ::
          node_id | :random | :controller | {:topic_partition, topic_name, partition_id}

  @typedoc """
  Represents an API response message

  This will generally be a generated struct.
  """
  @type api_response :: map

  @doc """
  Produce messages to a Kafka topic.

  ## Options

  - `:version` - Protocol version to use (default: 1)
  - `:acks` - Acknowledgment level (default: -1 for all replicas)
  - `:timeout` - Request timeout in ms (default: 1000)
  """
  def produce(client_pid, record_batch, topic, partition, acks, timeout)
      when is_integer(acks) and is_integer(timeout) do
    produce(client_pid, record_batch, topic, partition, acks: acks, timeout: timeout)
  end

  def produce(client_pid, record_batch, topic, partition, opts \\ []) when is_list(opts) do
    version = Keyword.get(opts, :version, 1)
    acks = Keyword.get(opts, :acks, -1)
    timeout = Keyword.get(opts, :timeout, 1_000)

    request = Kayrock.Produce.get_request_struct(version)

    request = %{
      request
      | acks: acks,
        timeout: timeout,
        topic_data: [
          %{
            topic: topic,
            data: [
              %{partition: partition, record_set: record_batch}
            ]
          }
        ]
    }

    client_call(client_pid, request, {:topic_partition, topic, partition})
  end

  @doc """
  Fetch messages from a Kafka topic.

  ## Options

  - `:version` - Protocol version to use (default: 4)
  - `:max_wait_time` - Maximum wait time in ms (default: 1000)
  - `:min_bytes` - Minimum bytes to return (default: 0)
  - `:max_bytes` - Maximum bytes to return (default: 1_000_000)
  - `:isolation_level` - 0 = read uncommitted, 1 = read committed (default: 1)
  """
  def fetch(client_pid, topic, partition, offset, opts \\ []) do
    version = Keyword.get(opts, :version, 4)
    max_wait_time = Keyword.get(opts, :max_wait_time, 1000)
    min_bytes = Keyword.get(opts, :min_bytes, 0)
    max_bytes = Keyword.get(opts, :max_bytes, 1_000_000)
    isolation_level = Keyword.get(opts, :isolation_level, 1)

    request = Kayrock.Fetch.get_request_struct(version)

    base_fields = %{
      replica_id: -1,
      max_wait_time: max_wait_time,
      min_bytes: min_bytes,
      max_bytes: max_bytes,
      topics: [
        %{
          topic: topic,
          partitions: [
            %{partition: partition, fetch_offset: offset, partition_max_bytes: max_bytes}
          ]
        }
      ]
    }

    # Add isolation_level for V4+
    fields =
      if version >= 4 do
        Map.put(base_fields, :isolation_level, isolation_level)
      else
        base_fields
      end

    request = struct(request, fields)

    client_call(client_pid, request, {:topic_partition, topic, partition})
  end

  @doc """
  Fetch metadata for topics from the Kafka cluster.

  ## Parameters

  - `client_pid` - PID of a `Kayrock.Client` process
  - `topics` - List of topic names, or `nil` for all topics
  """
  def topics_metadata(client_pid, topics) when is_list(topics) or topics == nil do
    # we use version 4 here so that it will not try to create topics
    topics_param = if topics, do: Enum.map(topics, fn topic -> %{name: topic} end), else: nil
    request = %Kayrock.Metadata.V4.Request{topics: topics_param}
    {:ok, metadata} = client_call(client_pid, request, :controller)
    {:ok, metadata.topics}
  end

  @doc """
  Fetch the supported API versions from the cluster
  Kayrock currently supports versions 0 and 1.
  [ApiVersions](https://kafka.apache.org/protocol.html#The_Messages_ApiVersions)
  """
  @spec api_versions(client_pid, api_version, node_selector) :: {:ok, api_response}
  def api_versions(client_pid, version \\ 0, node_selector \\ :controller) do
    request = Kayrock.ApiVersions.get_request_struct(version)
    client_call(client_pid, request, node_selector)
  end

  @doc """
  Create one or more topics
  """
  def create_topics(client_pid, topics, timeout \\ -1, version \\ 2, node_selector \\ :controller) do
    topic_requests =
      for topic <- topics do
        build_create_topic_request(topic)
      end

    request = Kayrock.CreateTopics.get_request_struct(version)
    request = %{request | topics: topic_requests, timeout_ms: timeout}

    client_call(client_pid, request, node_selector)
  end

  @doc """
  Delete topics from the Kafka cluster.
  """
  def delete_topics(client_pid, topics, timeout \\ -1, version \\ 1, node_selector \\ :controller) do
    request = Kayrock.DeleteTopics.get_request_struct(version)
    request = %{request | topic_names: topics, timeout_ms: timeout}

    client_call(client_pid, request, node_selector)
  end

  @doc """
  Make an api call to a Kafka cluster mediated through a client
  """
  @spec client_call(client_pid, Request.t(), node_selector) :: {:ok, api_response}
  def client_call(client_pid, request, node_selector \\ :random) when is_pid(client_pid) do
    Client.broker_call(client_pid, node_selector, request)
  end

  @doc """
  Makes a synchronous call directly to a broker

  `broker_id` should be the pid of a `Kayrock.BrokerConnection`.  `request` must
  have an implementation for the `Kayrock.Request` protocol.
  """
  @spec broker_sync_call(pid, Request.t()) :: {:ok, map}
  def broker_sync_call(
        broker_pid,
        %{correlation_id: correlation_id, client_id: client_id} = request
      )
      when is_pid(broker_pid) and is_integer(correlation_id) and correlation_id >= 0 and
             is_binary(client_id) do
    wire_protocol = Request.serialize(request)
    response_deserializer = Request.response_deserializer(request)

    :ok = BrokerConnection.send(broker_pid, wire_protocol)
    {:ok, resp} = BrokerConnection.recv(broker_pid)
    {deserialized_resp, _} = response_deserializer.(resp)

    {:ok, deserialized_resp}
  end

  defp build_create_topic_request(topic) when is_map(topic) do
    defaults = %{num_partitions: -1, replication_factor: -1, assignments: [], configs: []}

    topic
    |> Map.put(:name, Map.get(topic, :topic, Map.get(topic, :name)))
    |> Map.delete(:topic)
    |> then(fn t ->
      t
      |> Map.put(:assignments, Map.get(t, :replica_assignment, Map.get(t, :assignments, [])))
      |> Map.delete(:replica_assignment)
      |> Map.put(:configs, Map.get(t, :config_entries, Map.get(t, :configs, [])))
      |> Map.delete(:config_entries)
    end)
    |> then(fn t -> Map.merge(defaults, t) end)
  end
end
