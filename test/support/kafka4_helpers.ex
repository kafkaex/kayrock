defmodule Kayrock.Kafka4Helpers do
  @moduledoc """
  Helpers for tests against `Kayrock.Kafka4Container` (Apache Kafka 4.x,
  KRaft). Imported by `Kayrock.Kafka4Case`.

  Kafka 4.x removed many old protocol versions (KIP-896), so these helpers
  pick request versions the 4.x broker accepts (e.g. CreateTopics v5,
  Metadata v9, ApiVersions v3) instead of the v0-v2 defaults used by
  `Kayrock.IntegrationHelpers`.

  ## Available helpers

    * `build_client/1` (from `Kayrock.IntegrationHelpers`) - a `Kayrock.Client`
      connected to the container
    * `bootstrap_uri/1` - `{host, port}` of the container's client listener
    * `connect_broker/1` - a raw `Kayrock.BrokerConnection` to the container
    * `create_kafka4_topic/2` - create a topic (partitions / configs) and wait
      until it has leaders
    * `broker_api_versions/1` - the broker's advertised ranges,
      `%{api_key => {min, max}}`
    * `broker_supports?/3` - whether the broker accepts `api` at `version`
  """

  import Kayrock.TestSupport, only: [unique_string: 0]

  alias Kayrock.Kafka4Container
  alias Testcontainers.Container

  @doc "`{host, port}` of the container's client listener as seen from the test VM."
  @spec bootstrap_uri(Container.t()) :: {String.t(), pos_integer()}
  def bootstrap_uri(kafka) do
    {Testcontainers.get_host(), Container.mapped_port(kafka, Kafka4Container.kafka_port())}
  end

  @doc """
  Opens a raw `Kayrock.BrokerConnection` to the container (for tests that
  need to bypass `Kayrock.Client`, e.g. to send a request at a version the
  client would not pick). Use with `Kayrock.broker_sync_call/2`; requests
  need `correlation_id` and `client_id` set.
  """
  @spec connect_broker(Container.t()) :: {:ok, pid()}
  def connect_broker(kafka) do
    {host, port} = bootstrap_uri(kafka)
    Kayrock.BrokerConnection.start_link(host, port, [])
  end

  @doc """
  Creates a topic with CreateTopics v5 and waits until every partition has
  a leader.

  ## Options

    * `:name` - topic name (default: a unique random string)
    * `:partitions` - number of partitions (default: 3)
    * `:replication_factor` - (default: 1)
    * `:configs` - topic configs, map or keyword (default: none)
    * `:timeout_ms` - CreateTopics timeout (default: 10_000)

  Returns the topic name; raises if the broker reports an error.
  """
  @spec create_kafka4_topic(pid(), keyword()) :: String.t()
  def create_kafka4_topic(client, opts \\ []) do
    name = Keyword.get(opts, :name, unique_string())
    partitions = Keyword.get(opts, :partitions, 3)

    configs =
      opts
      |> Keyword.get(:configs, [])
      |> Enum.map(fn {k, v} -> %{name: to_string(k), value: to_string(v), tagged_fields: []} end)

    request = %Kayrock.CreateTopics.V5.Request{
      topics: [
        %{
          name: name,
          num_partitions: partitions,
          replication_factor: Keyword.get(opts, :replication_factor, 1),
          assignments: [],
          configs: configs,
          tagged_fields: []
        }
      ],
      timeout_ms: Keyword.get(opts, :timeout_ms, 10_000),
      validate_only: false,
      tagged_fields: []
    }

    {:ok, %{topics: [%{name: ^name, error_code: error_code} = result]}} =
      Kayrock.client_call(client, request, :controller)

    if error_code != 0 do
      raise "CreateTopics #{name} failed: #{inspect(result)}"
    end

    :ok = wait_for_leaders(client, name, partitions)
    name
  end

  @doc """
  Polls Metadata v9 until `topic` reports `partitions` partitions, each with
  a leader.
  """
  @spec wait_for_leaders(pid(), String.t(), pos_integer(), non_neg_integer()) ::
          :ok | {:error, :timeout}
  def wait_for_leaders(client, topic, partitions, retries \\ 50)

  def wait_for_leaders(_client, _topic, _partitions, 0), do: {:error, :timeout}

  def wait_for_leaders(client, topic, partitions, retries) do
    request = %Kayrock.Metadata.V9.Request{
      topics: [%{name: topic, tagged_fields: []}],
      allow_auto_topic_creation: false,
      include_cluster_authorized_operations: false,
      include_topic_authorized_operations: false,
      tagged_fields: []
    }

    ready? =
      case Kayrock.client_call(client, request, :controller) do
        {:ok, %{topics: [%{error_code: 0, partitions: parts}]}} ->
          length(parts) == partitions and Enum.all?(parts, &(&1.leader_id >= 0))

        _ ->
          false
      end

    if ready? do
      :ok
    else
      Process.sleep(100)
      wait_for_leaders(client, topic, partitions, retries - 1)
    end
  end

  @doc """
  The broker's advertised API version ranges from an ApiVersions v3 request:
  `%{api_key => {min_version, max_version}}`.
  """
  @spec broker_api_versions(pid()) :: %{non_neg_integer() => {integer(), integer()}}
  def broker_api_versions(client) do
    request = %{
      Kayrock.ApiVersions.get_request_struct(3)
      | client_software_name: "kayrock-test",
        client_software_version: "kafka4",
        tagged_fields: []
    }

    {:ok, %{error_code: 0, api_keys: api_keys}} = Kayrock.client_call(client, request, :random)
    Map.new(api_keys, &{&1.api_key, {&1.min_version, &1.max_version}})
  end

  @doc """
  Whether the broker (as described by `broker_api_versions/1`) accepts
  `api` (atom, e.g. `:fetch`) at `version`.
  """
  @spec broker_supports?(map(), atom(), non_neg_integer()) :: boolean()
  def broker_supports?(broker_versions, api, version) do
    case Map.fetch(broker_versions, Kayrock.KafkaSchemaMetadata.api_key(api)) do
      {:ok, {min, max}} -> version >= min and version <= max
      :error -> false
    end
  end
end
