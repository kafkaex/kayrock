defmodule Kayrock.Kafka4.DescribeClusterTest do
  @moduledoc """
  DescribeCluster v0-v2 against apache/kafka:4.4.0. Every response must
  decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @mismatched_endpoint_type 114
  @brokers 1
  @controllers 2

  defp request(0, include_ops),
    do: %Kayrock.DescribeCluster.V0.Request{include_cluster_authorized_operations: include_ops}

  defp request(1, include_ops),
    do: %Kayrock.DescribeCluster.V1.Request{
      include_cluster_authorized_operations: include_ops,
      endpoint_type: @brokers
    }

  defp request(2, include_ops),
    do: %Kayrock.DescribeCluster.V2.Request{
      include_cluster_authorized_operations: include_ops,
      endpoint_type: @brokers,
      include_fenced_brokers: true
    }

  for version <- 0..2 do
    test "v#{version} describes the single-node cluster", %{kafka: kafka} do
      version = unquote(version)
      {expected_host, expected_port} = bootstrap_uri(kafka)
      cluster_id = cluster_id()
      node_id = node_id()

      assert {response, <<>>} = raw_call(kafka, request(version, true))
      assert response.error_code == 0
      assert response.error_message == nil
      assert response.cluster_id == cluster_id
      assert response.controller_id == node_id
      assert response.cluster_authorized_operations != -2_147_483_648

      assert [
               %{broker_id: ^node_id, host: ^expected_host, port: ^expected_port, rack: nil} =
                 broker
             ] =
               response.brokers

      if version >= 1, do: assert(response.endpoint_type == @brokers)
      if version >= 2, do: assert(broker.is_fenced == 0)

      assert {without_ops, <<>>} = raw_call(kafka, request(version, false))
      assert without_ops.cluster_authorized_operations == -2_147_483_648
    end
  end

  test "v1 asking a broker for controllers is MISMATCHED_ENDPOINT_TYPE", %{kafka: kafka} do
    request = %Kayrock.DescribeCluster.V1.Request{
      include_cluster_authorized_operations: false,
      endpoint_type: @controllers
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == @mismatched_endpoint_type
    assert is_binary(response.error_message)
    assert response.brokers == []
  end
end
