defmodule Kayrock.Kafka4.AlterPartitionReassignmentsJsonVersionsTest do
  @moduledoc """
  AlterPartitionReassignments v1 against apache/kafka:4.4.0. On a single
  node the only possible reassignment is onto the current replica set; a
  cancellation (null replicas) with nothing in progress is answered with
  NO_REASSIGNMENT_IN_PROGRESS (85). Responses must decode with no leftover
  bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @no_reassignment_in_progress 85

  defp request(topic, replicas, allow_rf_change) do
    %Kayrock.AlterPartitionReassignments.V1.Request{
      timeout_ms: 10_000,
      allow_replication_factor_change: allow_rf_change,
      topics: [%{name: topic, partitions: [%{partition_index: 0, replicas: replicas}]}]
    }
  end

  test "v1 reassigns onto the current replicas", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)

    assert {response, <<>>} = raw_call(kafka, request(topic, [node_id()], false))
    assert response.error_code == 0
    assert response.allow_replication_factor_change == 0

    assert [%{name: ^topic, partitions: [%{partition_index: 0, error_code: 0}]}] =
             response.responses
  end

  test "v1 cancelling with nothing in progress", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)

    assert {response, <<>>} = raw_call(kafka, request(topic, nil, true))
    assert response.error_code == 0

    assert [%{partitions: [%{error_code: @no_reassignment_in_progress}]}] = response.responses
  end
end
