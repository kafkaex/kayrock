defmodule Kayrock.Kafka4.DescribeTopicPartitionsTest do
  @moduledoc """
  DescribeTopicPartitions v0 against apache/kafka:4.4.0, including cursor
  based paging (`ResponsePartitionLimit` / `NextCursor`). Every response must
  decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers
  import Kayrock.TestSupport, only: [unique_string: 0]

  @unknown_topic_or_partition 3

  defp request(topics, limit, cursor) do
    %Kayrock.DescribeTopicPartitions.V0.Request{
      topics: Enum.map(topics, &%{name: &1}),
      response_partition_limit: limit,
      cursor: cursor
    }
  end

  test "v0 describes a topic", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 3)
    node_id = node_id()

    assert {response, <<>>} = raw_call(kafka, request([topic], 2000, nil))
    assert response.next_cursor == nil
    assert [described] = response.topics
    assert %{error_code: 0, name: ^topic, is_internal: 0} = described
    assert described.topic_id != <<0::128>>
    assert described.topic_authorized_operations != -2_147_483_648

    assert described.partitions |> Enum.map(& &1.partition_index) |> Enum.sort() == [0, 1, 2]

    for partition <- described.partitions do
      assert %{
               error_code: 0,
               leader_id: ^node_id,
               replica_nodes: [^node_id],
               isr_nodes: [^node_id]
             } =
               partition

      assert partition.leader_epoch >= 0
      assert partition.offline_replicas == []
    end
  end

  test "v0 pages with a cursor", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 3)

    assert {first, <<>>} = raw_call(kafka, request([topic], 2, nil))
    assert [%{partitions: [%{partition_index: 0}, %{partition_index: 1}]}] = first.topics
    assert %{topic_name: ^topic, partition_index: 2} = first.next_cursor

    cursor = Map.take(first.next_cursor, [:topic_name, :partition_index])
    assert {second, <<>>} = raw_call(kafka, request([topic], 2, cursor))
    assert [%{name: ^topic, partitions: [%{partition_index: 2}]}] = second.topics
    assert second.next_cursor == nil
  end

  test "v0 unknown topic", %{kafka: kafka} do
    name = "missing-#{unique_string()}"
    assert {response, <<>>} = raw_call(kafka, request([name], 10, nil))

    assert [%{name: ^name, error_code: @unknown_topic_or_partition, partitions: []}] =
             response.topics
  end
end
