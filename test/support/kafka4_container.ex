defmodule Kayrock.Kafka4Container do
  @moduledoc """
  Testcontainers builder for a single-node KRaft Apache Kafka 4.x broker
  (`apache/kafka`, default `apache/kafka:4.4.0`).

  This sits alongside `Testcontainers.KafkaContainer` (Confluent 7.4 /
  ZooKeeper, used by the `:integration`, `:sanity` and `:chaos` suites); it
  does not replace it. Tests that use it are tagged `:kafka4` via
  `Kayrock.Kafka4Case` and run with `mix test.kafka4`.

  The node runs `broker,controller` in combined mode with three listeners:

    * `PLAINTEXT` on container port 9092 - the client listener, advertised
      as `<docker host>:<mapped port>` so clients on the host can reach it;
    * `BROKER` on 29092 - the inter-broker listener;
    * `CONTROLLER` on 9093 - the KRaft controller listener.

  Because the advertised client address depends on the mapped host port, the
  broker configuration is written into the container in `after_start/3` (the
  container command waits for that file), the storage directory is formatted
  with `kafka-storage.sh format`, and then the broker starts. The container is
  reported ready once an ApiVersions request succeeds through the mapped port.

  ## Usage

      container(:kafka, Kayrock.Kafka4Container.new(), shared: true)

      # with broker config overrides (later chunks: share/streams groups, ...)
      container(
        :kafka,
        Kayrock.Kafka4Container.new()
        |> Kayrock.Kafka4Container.with_broker_config(%{
          "group.consumer.heartbeat.interval.ms" => "1000"
        })
        |> Kayrock.Kafka4Container.with_feature("share.version", 1),
        shared: true
      )

  Broker config keys are Kafka property names (`server.properties` syntax),
  not `KAFKA_*` environment variables. Overrides are merged over
  `default_broker_config/0`; a `nil` value removes a default.
  """

  alias Testcontainers.Container

  @default_image "apache/kafka:4.4.0"
  @kafka_port 9092
  @broker_port 29_092
  @controller_port 9093
  @node_id 1
  @default_cluster_id "4L6g3nShT-eMCtK--X86sw"
  @default_wait_timeout 120_000
  @start_file "kayrock-kafka4-start.sh"
  @config_path "/tmp/kayrock-kafka4/server.properties"
  @log_dir "/tmp/kayrock-kafka4/data"

  # Single-node friendly settings: every internal topic must be creatable with
  # one replica, and group rebalances should not be delayed.
  @default_broker_config %{
    "auto.create.topics.enable" => "false",
    "num.partitions" => "1",
    "offsets.topic.replication.factor" => "1",
    "offsets.topic.num.partitions" => "1",
    "transaction.state.log.replication.factor" => "1",
    "transaction.state.log.min.isr" => "1",
    "transaction.state.log.num.partitions" => "1",
    "share.coordinator.state.topic.replication.factor" => "1",
    "share.coordinator.state.topic.min.isr" => "1",
    "share.coordinator.state.topic.num.partitions" => "1",
    "group.initial.rebalance.delay.ms" => "0",
    "group.min.session.timeout.ms" => "100"
  }

  @enforce_keys [:image, :cluster_id, :wait_timeout]
  defstruct image: @default_image,
            cluster_id: @default_cluster_id,
            wait_timeout: @default_wait_timeout,
            broker_config: %{},
            features: [],
            release_version: nil

  @type t :: %__MODULE__{
          image: String.t(),
          cluster_id: String.t(),
          wait_timeout: pos_integer(),
          broker_config: %{optional(String.t()) => String.t() | nil},
          features: [{String.t(), non_neg_integer()}],
          release_version: String.t() | nil
        }

  @doc "A single-node KRaft `apache/kafka:4.4.0` broker with test-friendly defaults."
  @spec new() :: t()
  def new do
    %__MODULE__{
      image: @default_image,
      cluster_id: @default_cluster_id,
      wait_timeout: @default_wait_timeout
    }
  end

  @doc "The container port of the client (PLAINTEXT) listener."
  @spec kafka_port() :: pos_integer()
  def kafka_port, do: @kafka_port

  @doc "The image used by `new/0`."
  @spec default_image() :: String.t()
  def default_image, do: @default_image

  @doc """
  The broker config applied before any `with_broker_config/2` overrides.
  """
  @spec default_broker_config() :: %{String.t() => String.t()}
  def default_broker_config, do: @default_broker_config

  @doc "Uses another `apache/kafka` image (e.g. a different 4.x tag)."
  @spec with_image(t(), String.t()) :: t()
  def with_image(%__MODULE__{} = config, image) when is_binary(image) do
    %{config | image: image}
  end

  @doc """
  Merges broker config overrides (Kafka property names) over the defaults.

  Values are converted with `to_string/1`; `nil` removes a property
  (including a default one). May be called repeatedly; later calls win.
  """
  @spec with_broker_config(t(), map() | keyword()) :: t()
  def with_broker_config(%__MODULE__{} = config, overrides)
      when is_map(overrides) or is_list(overrides) do
    overrides =
      Map.new(overrides, fn {key, value} ->
        {to_string(key), if(is_nil(value), do: nil, else: to_string(value))}
      end)

    %{config | broker_config: Map.merge(config.broker_config, overrides)}
  end

  @doc """
  Sets a KRaft feature level at storage-format time
  (`kafka-storage.sh format --feature name=level`), e.g.
  `with_feature(config, "share.version", 1)`.
  """
  @spec with_feature(t(), String.t(), non_neg_integer()) :: t()
  def with_feature(%__MODULE__{} = config, name, level)
      when is_binary(name) and is_integer(level) and level >= 0 do
    features = List.keystore(config.features, name, 0, {name, level})
    %{config | features: features}
  end

  @doc """
  Formats storage with an explicit `--release-version` (e.g. `"3.9-IV0"`)
  instead of the image's latest metadata version.
  """
  @spec with_release_version(t(), String.t()) :: t()
  def with_release_version(%__MODULE__{} = config, release_version)
      when is_binary(release_version) do
    %{config | release_version: release_version}
  end

  @doc "Overrides the KRaft cluster id used to format storage."
  @spec with_cluster_id(t(), String.t()) :: t()
  def with_cluster_id(%__MODULE__{} = config, cluster_id) when is_binary(cluster_id) do
    %{config | cluster_id: cluster_id}
  end

  @doc "Overrides how long to wait for the broker to answer ApiVersions."
  @spec with_wait_timeout(t(), pos_integer()) :: t()
  def with_wait_timeout(%__MODULE__{} = config, timeout)
      when is_integer(timeout) and timeout > 0 do
    %{config | wait_timeout: timeout}
  end

  @doc """
  The effective `server.properties` content for a broker whose client
  listener is advertised as `advertised_host:advertised_port` and whose
  container IP is `internal_host`.
  """
  @spec server_properties(t(), String.t(), pos_integer(), String.t()) :: String.t()
  def server_properties(%__MODULE__{} = config, advertised_host, advertised_port, internal_host) do
    base = %{
      "process.roles" => "broker,controller",
      "node.id" => "#{@node_id}",
      "controller.quorum.voters" => "#{@node_id}@localhost:#{@controller_port}",
      "listeners" =>
        "PLAINTEXT://0.0.0.0:#{@kafka_port},BROKER://0.0.0.0:#{@broker_port}," <>
          "CONTROLLER://0.0.0.0:#{@controller_port}",
      "advertised.listeners" =>
        "PLAINTEXT://#{advertised_host}:#{advertised_port}," <>
          "BROKER://#{internal_host}:#{@broker_port}",
      "listener.security.protocol.map" =>
        "PLAINTEXT:PLAINTEXT,BROKER:PLAINTEXT,CONTROLLER:PLAINTEXT",
      "inter.broker.listener.name" => "BROKER",
      "controller.listener.names" => "CONTROLLER",
      "log.dirs" => @log_dir
    }

    base
    |> Map.merge(@default_broker_config)
    |> Map.merge(config.broker_config)
    |> Enum.reject(fn {_key, value} -> is_nil(value) end)
    |> Enum.sort()
    |> Enum.map_join("\n", fn {key, value} -> "#{key}=#{value}" end)
  end

  @doc false
  @spec start_script(t(), String.t()) :: String.t()
  def start_script(%__MODULE__{} = config, server_properties) do
    format_args =
      [
        "--config #{@config_path}",
        "--cluster-id '#{config.cluster_id}'",
        "--ignore-formatted"
      ] ++
        if(config.release_version,
          do: ["--release-version '#{config.release_version}'"],
          else: []
        ) ++
        Enum.map(config.features, fn {name, level} -> "--feature '#{name}=#{level}'" end)

    """
    set -e
    mkdir -p #{Path.dirname(@config_path)} #{@log_dir}
    cat > #{@config_path} <<'KAYROCK_EOF'
    #{server_properties}
    KAYROCK_EOF
    /opt/kafka/bin/kafka-storage.sh format #{Enum.join(format_args, " ")}
    exec /opt/kafka/bin/kafka-server-start.sh #{@config_path}
    """
  end

  defimpl Testcontainers.ContainerBuilder do
    alias Kayrock.Kafka4Container
    alias Testcontainers.Container
    alias Testcontainers.Docker

    @impl true
    def build(%Kafka4Container{} = config) do
      config.image
      |> Container.new()
      |> Container.with_exposed_port(Kafka4Container.kafka_port())
      |> Container.with_cmd([
        "sh",
        "-c",
        "while [ ! -f /#{Kafka4Container.start_file()} ]; do sleep 0.1; done; " <>
          "exec sh /#{Kafka4Container.start_file()}"
      ])
      |> Container.with_waiting_strategy(
        Kafka4Container.ApiVersionsWaitStrategy.new(
          Kafka4Container.kafka_port(),
          config.wait_timeout
        )
      )
    end

    @impl true
    def after_start(%Kafka4Container{} = config, container, conn) do
      host = Testcontainers.get_host()
      port = Container.mapped_port(container, Kafka4Container.kafka_port())
      internal_host = container.ip_address || "localhost"

      properties = Kafka4Container.server_properties(config, host, port, internal_host)
      script = Kafka4Container.start_script(config, properties)

      Docker.Api.put_file(container.container_id, conn, "/", Kafka4Container.start_file(), script)
    end
  end

  @doc false
  @spec start_file() :: String.t()
  def start_file, do: @start_file

  defmodule ApiVersionsWaitStrategy do
    @moduledoc """
    Waits until the broker answers an ApiVersions v0 request with
    `error_code: 0` on the mapped client port.

    Unlike `Testcontainers.PortWaitStrategy` this proves the broker (not just
    the Docker port proxy) is serving requests.
    """

    @retry_delay 500
    defstruct [:port, :timeout, retry_delay: @retry_delay]

    @type t :: %__MODULE__{
            port: pos_integer(),
            timeout: pos_integer(),
            retry_delay: pos_integer()
          }

    @spec new(pos_integer(), pos_integer()) :: t()
    def new(port, timeout), do: %__MODULE__{port: port, timeout: timeout}

    @doc """
    Sends a raw ApiVersions v0 request to `host:port`; `:ok` when the broker
    answers with error code 0.
    """
    @spec probe(String.t(), pos_integer()) :: :ok | {:error, term()}
    def probe(host, port) do
      client_id = "kayrock-wait"
      body = <<18::16, 0::16, 0::32, byte_size(client_id)::16, client_id::binary>>
      opts = [:binary, active: false, packet: 4]

      case :gen_tcp.connect(String.to_charlist(host), port, opts, 2_000) do
        {:ok, socket} ->
          result = send_api_versions(socket, body)
          :gen_tcp.close(socket)
          result

        {:error, _} = error ->
          error
      end
    end

    defp send_api_versions(socket, body) do
      with :ok <- :gen_tcp.send(socket, body),
           {:ok, <<0::32, error_code::16-signed, _::binary>>} <- :gen_tcp.recv(socket, 0, 5_000) do
        if error_code == 0, do: :ok, else: {:error, {:kafka_error, error_code}}
      end
    end

    defimpl Testcontainers.WaitStrategy do
      alias Kayrock.Kafka4Container.ApiVersionsWaitStrategy
      alias Testcontainers.Container

      @impl true
      def wait_until_container_is_ready(strategy, container, _conn) do
        host = Testcontainers.get_host()
        port = Container.mapped_port(container, strategy.port)
        deadline = System.monotonic_time(:millisecond) + strategy.timeout
        wait(strategy, host, port, deadline, nil)
      end

      defp wait(strategy, host, port, deadline, last_error) do
        case ApiVersionsWaitStrategy.probe(host, port) do
          :ok ->
            :ok

          {:error, reason} ->
            if System.monotonic_time(:millisecond) > deadline do
              {:error, {:kafka4_not_ready, last_error || reason}, strategy}
            else
              Process.sleep(strategy.retry_delay)
              wait(strategy, host, port, deadline, reason)
            end
        end
      end
    end
  end
end
