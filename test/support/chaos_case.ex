defmodule Kayrock.ChaosCase do
  @moduledoc """
  Chaos engineering test case template.

  Provides infrastructure for chaos tests with Toxiproxy including:
  - Testcontainers setup (Kafka + Toxiproxy) on a shared Docker network
  - Kafka configured to advertise proxy port so ALL traffic goes through proxy
  - Chaos helper functions
  - Common test patterns

  ## Architecture

      Test Client --> Toxiproxy (localhost:PROXY_PORT) --> Kafka (kafka:9092)

  Kafka is configured to advertise the proxy port, so all client connections
  go through the proxy. This enables timeout toxics to work correctly.

  ## Usage

      defmodule MyChaosTest do
        use Kayrock.ChaosCase
        use ExUnit.Case, async: false

        test "scenario", %{client: client, toxiproxy: toxiproxy, proxy_name: proxy_name} do
          add_latency(toxiproxy, proxy_name, 1000)
          # ... test logic
        end
      end

  ## Test Categories

  - Unit tests: `mix test --exclude integration --exclude chaos`
  - Integration tests: `mix test --only integration`
  - Chaos tests: `mix test --only chaos`
  """
  use ExUnit.CaseTemplate

  require Logger

  alias Testcontainers.Container
  alias Testcontainers.Docker
  alias Testcontainers.ToxiproxyContainer

  @kafka_image "confluentinc/cp-kafka:7.4.3"
  @kafka_port 9092
  @kafka_broker_port 29092
  @zookeeper_port 2181
  @start_script_path "tc-start.sh"

  using do
    quote do
      @moduletag :chaos
      @moduletag timeout: 180_000

      import Kayrock.IntegrationHelpers
      import Kayrock.ToxiproxyHelpers
      import Kayrock.TestSupport
      import Kayrock.RequestFactory
      import Kayrock.ChaosTestHelpers

      alias Testcontainers.Container
      alias Testcontainers.ToxiproxyContainer

      setup_all do
        # Start infrastructure for all tests in this module
        {:ok, ctx} = Kayrock.ChaosCase.start_infrastructure()

        on_exit(fn ->
          Kayrock.ChaosCase.stop_infrastructure(ctx)
        end)

        {:ok, ctx}
      end

      setup %{toxiproxy: toxiproxy, proxy_name: proxy_name, broker_via_proxy: broker} do
        # Reset toxics before each test
        :ok = ToxiproxyContainer.reset(toxiproxy)

        # Create fresh client for each test
        {:ok, client_pid} = Kayrock.Client.start_link([broker])

        # Unlink the client so test doesn't crash if client dies under chaos
        # This allows tests to verify error handling without cascading failures
        Process.unlink(client_pid)

        on_exit(fn ->
          # Safe cleanup that handles stuck clients under chaos conditions
          if Process.alive?(client_pid) do
            try do
              GenServer.stop(client_pid, :normal, 1000)
            catch
              :exit, _ ->
                # Client is stuck (likely due to timeout toxic), force kill it
                Process.exit(client_pid, :kill)
            end
          end
        end)

        %{client: client_pid}
      end
    end
  end

  @doc """
  Start Kafka and Toxiproxy containers on a shared network for testing.

  Kafka is configured to advertise the proxy port, so all client traffic
  goes through Toxiproxy.

  Returns a context map with:
  - `:kafka_container` - the Kafka container
  - `:toxiproxy` - the Toxiproxy container (aliased for backwards compatibility)
  - `:network_name` - the Docker network name
  - `:proxy_name` - the name of the proxy in Toxiproxy
  - `:broker_via_proxy` - the broker address through the proxy `{host, port}`
  - `:proxy_port` - the proxy port
  """
  def start_infrastructure(opts \\ []) do
    # Handle Testcontainers already started (when multiple test suites run)
    case Testcontainers.start() do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    network_name = Keyword.get(opts, :network, "kayrock-chaos-#{:rand.uniform(100_000)}")

    {:ok, _} = Testcontainers.create_network(network_name)
    Logger.info("Created Docker network: #{network_name}")
    host = Testcontainers.get_host()

    # Step 1: Start Toxiproxy
    toxiproxy_config =
      Container.new(ToxiproxyContainer.default_image())
      |> Container.with_exposed_ports([
        ToxiproxyContainer.control_port() | Enum.to_list(8666..8696)
      ])
      |> Container.with_waiting_strategy(
        Testcontainers.PortWaitStrategy.new(
          "127.0.0.1",
          ToxiproxyContainer.control_port(),
          120_000
        )
      )
      |> Container.with_network(network_name)
      |> Container.with_hostname("toxiproxy")

    {:ok, toxiproxy_container} = Testcontainers.start_container(toxiproxy_config)
    Logger.info("Started Toxiproxy container")

    :ok = ToxiproxyContainer.configure_toxiproxy_ex(toxiproxy_container)
    Logger.info("Toxiproxy API ready at #{ToxiproxyContainer.api_url(toxiproxy_container)}")

    # Step 2: Start Kafka using startup script approach for dynamic advertised listeners
    kafka_config = build_kafka_container(network_name)
    {:ok, kafka_container} = Testcontainers.start_container(kafka_config)
    kafka_ip = kafka_container.ip_address
    Logger.info("Started Kafka container (IP: #{kafka_ip})")

    # Step 3: Configure proxy to route to Kafka's internal IP address
    proxy_name = "kafka_proxy"
    upstream = "#{kafka_ip}:#{@kafka_port}"

    {:ok, proxy_port} = ToxiproxyContainer.create_proxy(toxiproxy_container, proxy_name, upstream)
    Logger.info("Created proxy: #{proxy_name} -> #{upstream}")

    # Step 4: Upload the startup script with correct advertised listeners pointing to PROXY
    # This is the key - advertise the PROXY port so all client traffic goes through Toxiproxy
    :ok = upload_kafka_startup_script(kafka_container, kafka_ip, host, proxy_port)
    Logger.info("Uploaded startup script with advertised listener: #{host}:#{proxy_port}")

    # Step 5: Wait for Kafka to be ready (through the proxy)
    :ok = wait_for_kafka(host, proxy_port)
    Logger.info("Kafka ready via proxy at #{host}:#{proxy_port}")

    {:ok,
     %{
       kafka_container: kafka_container,
       toxiproxy: toxiproxy_container,
       network_name: network_name,
       proxy_name: proxy_name,
       broker_via_proxy: {host, proxy_port},
       proxy_port: proxy_port
     }}
  end

  @doc """
  Stop infrastructure containers and remove network.
  """
  def stop_infrastructure(%{
        kafka_container: kafka,
        toxiproxy: toxiproxy,
        network_name: network_name
      }) do
    Testcontainers.stop_container(kafka.container_id)
    Testcontainers.stop_container(toxiproxy.container_id)
    Testcontainers.remove_network(network_name)
    :ok
  end

  def stop_infrastructure(_), do: :ok

  # Build Kafka container using startup script approach
  defp build_kafka_container(network_name) do
    Container.new(@kafka_image)
    |> Container.with_exposed_port(@kafka_port)
    |> Container.with_network(network_name)
    |> Container.with_hostname("kafka")
    |> Container.with_environment(
      "KAFKA_LISTENERS",
      "BROKER://0.0.0.0:#{@kafka_broker_port},OUTSIDE://0.0.0.0:#{@kafka_port}"
    )
    |> Container.with_environment(
      "KAFKA_LISTENER_SECURITY_PROTOCOL_MAP",
      "BROKER:PLAINTEXT,OUTSIDE:PLAINTEXT"
    )
    |> Container.with_environment("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_OFFSETS_TOPIC_NUM_PARTITIONS", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR", "1")
    |> Container.with_environment("KAFKA_TRANSACTION_STATE_LOG_MIN_ISR", "1")
    |> Container.with_environment("KAFKA_AUTO_CREATE_TOPICS_ENABLE", "true")
    |> Container.with_cmd([
      "sh",
      "-c",
      "while [ ! -f /#{@start_script_path} ]; do echo 'waiting for startup script...' && sleep 0.1; done; sh /#{@start_script_path};"
    ])
  end

  defp upload_kafka_startup_script(container, kafka_ip, proxy_host, proxy_port) do
    internal_listener = "BROKER://#{kafka_ip}:#{@kafka_broker_port}"
    external_listener = "OUTSIDE://#{proxy_host}:#{proxy_port}"

    script = """
    export KAFKA_BROKER_ID=1
    export KAFKA_ADVERTISED_LISTENERS=#{internal_listener},#{external_listener}
    echo '' > /etc/confluent/docker/ensure
    export KAFKA_ZOOKEEPER_CONNECT='localhost:#{@zookeeper_port}'
    echo 'clientPort=#{@zookeeper_port}' > zookeeper.properties
    echo 'dataDir=/var/lib/zookeeper/data' >> zookeeper.properties
    echo 'dataLogDir=/var/lib/zookeeper/log' >> zookeeper.properties
    zookeeper-server-start zookeeper.properties &
    /etc/confluent/docker/run
    echo finished
    """

    script_content =
      script
      |> String.split("\n")
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.join("\n")

    {:ok, conn} = get_docker_connection()
    Docker.Api.put_file(container.container_id, conn, "/", @start_script_path, script_content)
  end

  defp get_docker_connection do
    {conn, _url, _host} = Testcontainers.Connection.get_connection([])
    {:ok, conn}
  end

  defp wait_for_kafka(host, port, attempts \\ 120) do
    Logger.info(
      "Waiting for Kafka to be ready at #{host}:#{port} (attempt #{121 - attempts}/120)..."
    )

    case try_kafka_api_versions(host, port) do
      :ok ->
        Logger.info("Kafka is ready and responding to API requests!")
        # Wait for internal topics to initialize
        Logger.info("Waiting for Kafka internal topics to initialize...")
        Process.sleep(5000)

        case try_kafka_api_versions(host, port) do
          :ok ->
            Logger.info("Kafka confirmed ready after settling period")
            :ok

          {:error, reason} ->
            Logger.warning("Kafka became unavailable after settling: #{inspect(reason)}")
            {:error, reason}
        end

      {:error, reason} when attempts > 0 ->
        Logger.debug("Kafka not ready yet: #{inspect(reason)}")
        Process.sleep(1000)
        wait_for_kafka(host, port, attempts - 1)

      {:error, reason} ->
        {:error, {:kafka_not_ready, reason}}
    end
  end

  defp try_kafka_api_versions(host, port) do
    case :gen_tcp.connect(~c"#{host}", port, [:binary, active: false, packet: :raw], 5000) do
      {:ok, socket} ->
        client_id = "tc-wait"
        client_id_bytes = <<byte_size(client_id)::16>> <> client_id

        request_body = <<18::16-signed, 0::16-signed, 1::32-signed>> <> client_id_bytes

        request = <<byte_size(request_body)::32-signed>> <> request_body

        result =
          with :ok <- :gen_tcp.send(socket, request),
               {:ok,
                <<_size::32-signed, _correlation_id::32-signed, error_code::16-signed,
                  _rest::binary>>} <-
                 :gen_tcp.recv(socket, 0, 10_000) do
            if error_code == 0 do
              :ok
            else
              {:error, {:kafka_error, error_code}}
            end
          end

        :gen_tcp.close(socket)
        result

      {:error, reason} ->
        {:error, reason}
    end
  end
end
