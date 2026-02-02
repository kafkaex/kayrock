defmodule Kayrock.ChaosCase do
  @moduledoc """
  Chaos engineering test case template.

  Provides infrastructure for chaos tests with Toxiproxy including:
  - Testcontainers setup (Kafka + Toxiproxy)
  - Chaos helper functions
  - Common test patterns

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

  using do
    quote do
      @moduletag :chaos
      @moduletag timeout: 120_000

      import Testcontainers.ExUnit
      import Kayrock.IntegrationHelpers
      import Kayrock.ToxiproxyHelpers
      import Kayrock.TestSupport
      import Kayrock.RequestFactory

      alias Testcontainers.Container
      alias Testcontainers.KafkaContainer
      alias Testcontainers.ToxiproxyContainer

      # Don't share containers for chaos tests - each test needs clean state
      container(:kafka, KafkaContainer.new(), shared: false)
      container(:toxiproxy, ToxiproxyContainer.new(), shared: false)

      setup %{kafka: kafka, toxiproxy: toxiproxy} do
        # Create proxy to Kafka through Toxiproxy
        {:ok, proxy_port} =
          ToxiproxyContainer.create_proxy_for_container(
            toxiproxy,
            "kafka_proxy",
            kafka,
            9092
          )

        # Build client pointing to proxied Kafka
        host = Testcontainers.get_host()
        {:ok, client_pid} = Kayrock.Client.start_link([{host, proxy_port}])

        # Reset toxics before each test
        :ok = ToxiproxyContainer.reset(toxiproxy)

        %{
          client: client_pid,
          proxy_port: proxy_port,
          toxiproxy: toxiproxy,
          proxy_name: "kafka_proxy"
        }
      end
    end
  end
end
