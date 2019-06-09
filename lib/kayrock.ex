defmodule Kayrock do
  @moduledoc """
  Documentation for Kayrock.
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

  def produce(client_pid, record_batch, topic, partition, acks \\ -1, timeout \\ 1_000) do
    request = %Kayrock.Produce.V1.Request{
      acks: acks,
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

  def fetch(client_pid, topic, partition, offset) do
    request = %Kayrock.Fetch.V4.Request{
      replica_id: -1,
      max_wait_time: 1000,
      min_bytes: 0,
      max_bytes: 1_000_000,
      isolation_level: 1,
      topics: [
        %{
          topic: topic,
          partitions: [
            %{partition: partition, fetch_offset: offset, max_bytes: 1_000_000}
          ]
        }
      ]
    }

    client_call(client_pid, request, {:topic_partition, topic, partition})
  end

  def topics_metadata(client_pid, topics) when is_list(topics) or topics == nil do
    # we use version 4 here so that it will not try to create topics
    request = %Kayrock.Metadata.V4.Request{topics: topics}
    {:ok, metadata} = client_call(client_pid, request, :controller)
    {:ok, metadata.topic_metadata}
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
    create_topic_requests =
      for topic <- topics do
        build_create_topic_request(topic)
      end

    request = Kayrock.CreateTopics.get_request_struct(version)
    request = %{request | create_topic_requests: create_topic_requests, timeout: timeout}

    client_call(client_pid, request, node_selector)
  end

  def delete_topics(client_pid, topics, timeout \\ -1, version \\ 1, node_selector \\ :controller) do
    request = Kayrock.DeleteTopics.get_request_struct(version)
    request = %{request | topics: topics, timeout: timeout}

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
    Map.merge(
      %{num_partitions: -1, replication_factor: -1, replica_assignment: [], config_entries: []},
      topic
    )
  end
end
