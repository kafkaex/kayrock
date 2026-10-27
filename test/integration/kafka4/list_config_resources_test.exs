defmodule Kayrock.Kafka4.ListConfigResourcesTest do
  @moduledoc """
  ListConfigResources v0-v1 against apache/kafka:4.4.0. v0 only lists
  client-metrics resources (none on a fresh broker); v1 lists the requested
  resource types (topics are type 2). Every response must decode with no
  leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @topic 2
  @broker 4

  test "v0 lists client metrics resources", %{kafka: kafka} do
    assert {response, <<>>} = raw_call(kafka, %Kayrock.ListConfigResources.V0.Request{})
    assert response.error_code == 0
    assert response.config_resources == []
  end

  test "v1 lists topics and brokers", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    node = to_string(node_id())

    assert {response, <<>>} =
             raw_call(kafka, %Kayrock.ListConfigResources.V1.Request{
               resource_types: [@topic, @broker]
             })

    assert response.error_code == 0

    assert %{resource_name: topic, resource_type: @topic, tagged_fields: []} in response.config_resources

    assert %{resource_name: node, resource_type: @broker, tagged_fields: []} in response.config_resources

    assert {defaults, <<>>} =
             raw_call(kafka, %Kayrock.ListConfigResources.V1.Request{resource_types: []})

    assert defaults.error_code == 0
    assert Enum.any?(defaults.config_resources, &(&1.resource_name == topic))
  end
end
