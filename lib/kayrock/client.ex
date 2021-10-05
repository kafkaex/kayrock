defmodule Kayrock.Client do
  @moduledoc """
  Manages communication with a single cluster
  """

  defmodule Opts do
    @moduledoc false

    def connect_opts(opts), do: Keyword.get(opts, :connection, [])

    def metadata_update_interval(opts), do: Keyword.get(opts, :metadata_update_interval, 30_000)
  end

  defmodule Broker do
    @moduledoc false

    defstruct node_id: nil, host: nil, port: nil, rack: nil, pid: nil

    def put_pid(%__MODULE__{} = broker, pid), do: %{broker | pid: pid}
  end

  defmodule Topic do
    @moduledoc false

    defstruct name: nil, partition_leaders: %{}

    def from_topic_metadata(%{topic: name, partition_metadata: partition_metadata}) do
      partition_leaders =
        Enum.into(
          partition_metadata,
          %{},
          fn %{error_code: 0, leader: leader, partition: partition_id} ->
            {partition_id, leader}
          end
        )

      %__MODULE__{name: name, partition_leaders: partition_leaders}
    end
  end

  defmodule State do
    @moduledoc false
    defstruct opts: [],
              seed_broker_uris: [],
              correlation_id: 0,
              client_id: "kayrock",
              cluster_metadata: nil

    def connect_opts(%__MODULE__{opts: opts}) do
      Opts.connect_opts(opts)
    end

    # sets the correlation id and client id of the request,
    # increments the state's correlation id
    def request(
          %__MODULE__{correlation_id: correlation_id, client_id: client_id} = state,
          request
        ) do
      {%{state | correlation_id: correlation_id + 1},
       %{request | correlation_id: correlation_id, client_id: client_id}}
    end

    def get_and_update_cluster_metadata(
          %__MODULE__{cluster_metadata: cluster_metadata} = state,
          cb
        )
        when is_function(cb, 1) do
      {val, updated_cluster_metadata} = cb.(cluster_metadata)
      {val, %{state | cluster_metadata: updated_cluster_metadata}}
    end
  end

  use GenServer

  alias Kayrock.BrokerConnection
  alias Kayrock.Client.ClusterMetadata

  require Logger

  def start_link(seed_broker_uris \\ [{"localhost", 9092}], opts \\ []) do
    GenServer.start_link(__MODULE__, {seed_broker_uris, opts})
  end

  def broker_call(pid, node_selector, request) do
    GenServer.call(pid, {:broker_call, node_selector, request})
  end

  def cluster_metadata(pid) do
    GenServer.call(pid, :cluster_metadata)
  end

  def stop(pid) do
    GenServer.stop(pid)
  end

  @impl true
  def init({seed_broker_uris, opts}) do
    state = %State{
      opts: opts,
      seed_broker_uris: seed_broker_uris,
      correlation_id: 0,
      client_id: "kayrock"
    }

    cluster_metadata = metadata_from_seed(state)

    {:ok, _} = :timer.send_interval(Opts.metadata_update_interval(opts), :update_metadata)

    # correlation id should be 0 because we disconnected from the seed broker
    {:ok, %{state | cluster_metadata: cluster_metadata, correlation_id: 0}}
  end

  @impl true
  def handle_call({:broker_call, node_selector, request}, _from, state) do
    {state_out, request} = State.request(state, request)

    case find_node(state_out, node_selector) do
      {:ok, node_id, state_out} ->
        {broker_pid, state_out} = ensure_broker_connection(state_out, node_id)
        resp = Kayrock.broker_sync_call(broker_pid, request)
        {:reply, resp, state_out}

      {:error, error, state_out} ->
        {:reply, {:error, error}, state_out}
    end
  end

  def handle_call(:cluster_metadata, _from, state) do
    {:reply, {:ok, state.cluster_metadata}, state}
  end

  @impl true
  def handle_info(:update_metadata, state) do
    Logger.debug("Performing scheduled update of cluster metadata #{inspect(self())}")
    {:noreply, update_metadata(state)}
  end

  @impl true
  def terminate(_reason, state) do
    for {_node_id, broker} <- state.cluster_metadata.brokers do
      if broker.pid do
        BrokerConnection.stop(broker.pid)
      end
    end

    :ok
  end

  # called when we first start a client
  # uses a random seed broker to fetch cluster metadata.  we do not fetch any
  # topic metadata in this request because there could potentially be a large
  # number of topics on the cluster.
  defp metadata_from_seed(%State{} = state) do
    [{seed_host, seed_port}] = Enum.take_random(state.seed_broker_uris, 1)
    Logger.debug("Connecting to seed broker #{inspect(seed_host)}:#{inspect(seed_port)}")
    connect_opts = State.connect_opts(state)
    {:ok, seed_pid} = BrokerConnection.start_link(seed_host, seed_port, connect_opts)

    # ignore the state update since we're throwing away the connection
    {metadata, _} = fetch_metadata(seed_pid, state, [])

    :ok = BrokerConnection.stop(seed_pid)
    Logger.debug("Disconnected from seed broker #{inspect(seed_pid)}")

    ClusterMetadata.from_metadata_v1_response(metadata)
  end

  # update the state's metadata for the given list of topics
  # this gets called periodically to keep metadata fresh (see the
  # handle_info(:timeout, _) clause above) as well as when we try to interact
  # with a topic for which we don't yet have metadata
  defp update_metadata(state, topics \\ []) do
    {controller_pid, updated_state} =
      ensure_broker_connection(state, state.cluster_metadata.controller_id)

    # update existing topics as well as the new one
    topics = topics ++ ClusterMetadata.known_topics(state.cluster_metadata)

    {metadata_response, updated_state} = fetch_metadata(controller_pid, updated_state, topics)
    new_cluster_metadata = ClusterMetadata.from_metadata_v1_response(metadata_response)

    # we want to keep any broker connections that are still valid and close any
    # that are no longer appropriate
    {updated_cluster_metadata, brokers_to_close} =
      ClusterMetadata.merge_brokers(updated_state.cluster_metadata, new_cluster_metadata)

    for broker <- brokers_to_close do
      BrokerConnection.close(broker.pid)
    end

    %{updated_state | cluster_metadata: updated_cluster_metadata}
  end

  defp fetch_metadata(pid, %State{} = state, topics) do
    # fetch metadata from one of the bootstrap servers
    # NOTE we use V1 of the metadata request because it allows us to specify an
    # empty list of topics and signify that we don't want any topic metadata.
    # On larger clusters, requesting all for the topic metadata can lead to timeouts.
    {updated_state, metadata_request} =
      State.request(state, %Kayrock.Metadata.V1.Request{topics: topics})

    {:ok, metadata} = Kayrock.broker_sync_call(pid, metadata_request)
    {metadata, updated_state}
  end

  # checks for a connection to the given node; returns the pid if it is already
  # existed or makes a connection if not already connected
  defp ensure_broker_connection(%State{} = state, node_id) do
    State.get_and_update_cluster_metadata(state, fn cluster_metadata ->
      ClusterMetadata.get_and_update_broker(cluster_metadata, node_id, fn broker ->
        pid = broker_pid(broker, State.connect_opts(state))
        {pid, %{broker | pid: pid}}
      end)
    end)
  end

  # helper function, returns a pid if it's there or connects if not and returns
  # the new pid
  defp broker_pid(%Broker{pid: pid}, _) when is_pid(pid), do: pid

  defp broker_pid(%Broker{} = broker, connect_opts) do
    {:ok, pid} = BrokerConnection.start_link(broker.host, broker.port, connect_opts)
    pid
  end

  # given the node selector, get the corresponding node id
  # if we request a topic node and we don't yet have the topic metadata, updates
  # the metadata and returns the node id if one was found
  defp find_node(state, node_selector) do
    case ClusterMetadata.select_node(state.cluster_metadata, node_selector) do
      {:ok, node_id} ->
        {:ok, node_id, state}

      {:error, :no_such_topic} ->
        # we don't know about the topic yet, so try to fetch metadata for that
        # topic and then try again
        {_, topic, _} = node_selector
        updated_state = update_metadata(state, [topic])

        # should better handle if a topic is out of sync or being created

        case ClusterMetadata.select_node(updated_state.cluster_metadata, node_selector) do
          {:ok, node_id} ->
            {:ok, node_id, updated_state}

          other ->
            {:error, other, state}
        end

      {:error, other} ->
        {:error, other, state}
    end
  end
end
