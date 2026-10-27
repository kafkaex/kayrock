defmodule Kayrock.Kafka4Case do
  @moduledoc """
  ExUnit case template for tests against a single-node KRaft Apache Kafka 4.x
  broker (`Kayrock.Kafka4Container`, `apache/kafka:4.4.0` by default).

  Tags every test `:kafka4` (excluded by default, run with `mix test.kafka4`)
  and imports `Kayrock.Kafka4Helpers` and `Kayrock.IntegrationHelpers`.

  By default a broker with `Kayrock.Kafka4Container.new/0` defaults is
  started once per module and exposed as `context.kafka`. Pass `:container`
  to customise it (broker config overrides, KRaft features) or
  `container: false` to declare your own with `container/3`:

      defmodule MyKafka4Test do
        use Kayrock.Kafka4Case,
          container:
            Kayrock.Kafka4Container.new()
            |> Kayrock.Kafka4Container.with_broker_config(%{
              "group.consumer.heartbeat.interval.ms" => 1000
            })

        test "...", %{kafka: kafka} do
          {:ok, client} = build_client(kafka)
          topic = create_kafka4_topic(client, partitions: 2)
          ...
        end
      end

  Modules run synchronously (each owns a broker container).
  """
  use ExUnit.CaseTemplate

  using opts do
    container_config = Keyword.get(opts, :container, true)

    container_block =
      case container_config do
        false ->
          nil

        true ->
          quote do
            container(:kafka, Kayrock.Kafka4Container.new(), shared: true)
          end

        config ->
          quote do
            container(:kafka, unquote(config), shared: true)
          end
      end

    quote do
      @moduletag :kafka4
      @moduletag timeout: 300_000

      import Testcontainers.ExUnit
      import Kayrock.IntegrationHelpers
      import Kayrock.Kafka4Helpers

      alias Kayrock.Kafka4Container
      alias Testcontainers.Container

      unquote(container_block)
    end
  end
end
