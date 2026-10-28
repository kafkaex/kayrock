defmodule Kayrock.Kafka4ContainerTest do
  # Unit tests for the Kafka 4.x Testcontainers builder (no Docker needed).
  use ExUnit.Case, async: true

  alias Kayrock.Kafka4Container

  defp properties(config) do
    config
    |> Kafka4Container.server_properties("localhost", 32_768, "172.17.0.2")
    |> String.split("\n")
    |> Map.new(fn line ->
      [key, value] = String.split(line, "=", parts: 2)
      {key, value}
    end)
  end

  test "defaults to a single-node KRaft apache/kafka:4.4.0 broker" do
    config = Kafka4Container.new()
    assert config.image == "apache/kafka:4.4.0"

    props = properties(config)
    assert props["process.roles"] == "broker,controller"
    assert props["node.id"] == "1"
    assert props["controller.quorum.voters"] == "1@localhost:9093"
    assert props["advertised.listeners"] =~ "PLAINTEXT://localhost:32768"
    assert props["advertised.listeners"] =~ "BROKER://172.17.0.2:29092"
    assert props["offsets.topic.replication.factor"] == "1"
    assert props["auto.create.topics.enable"] == "false"
  end

  test "broker config overrides merge over defaults; nil removes a key" do
    props =
      Kafka4Container.new()
      |> Kafka4Container.with_broker_config(%{"auto.create.topics.enable" => true})
      |> Kafka4Container.with_broker_config(
        "group.share.record.lock.duration.ms": 15_000,
        "group.min.session.timeout.ms": nil
      )
      |> properties()

    assert props["auto.create.topics.enable"] == "true"
    assert props["group.share.record.lock.duration.ms"] == "15000"
    refute Map.has_key?(props, "group.min.session.timeout.ms")
  end

  test "features and release version are passed to kafka-storage format" do
    config =
      Kafka4Container.new()
      |> Kafka4Container.with_feature("share.version", 1)
      |> Kafka4Container.with_feature("streams.version", 1)
      |> Kafka4Container.with_feature("share.version", 0)
      |> Kafka4Container.with_release_version("4.0-IV3")

    script = Kafka4Container.start_script(config, "a=b")

    [format_line] =
      script |> String.split("\n") |> Enum.filter(&(&1 =~ "kafka-storage.sh format"))

    assert format_line =~ "--feature 'share.version=0'"
    assert format_line =~ "--feature 'streams.version=1'"
    refute format_line =~ "share.version=1"
    assert format_line =~ "--release-version '4.0-IV3'"
    assert format_line =~ "--cluster-id '#{config.cluster_id}'"
    assert script =~ "a=b\nKAYROCK_EOF"
    assert script =~ "exec /opt/kafka/bin/kafka-server-start.sh"
  end
end
