defmodule Kayrock.Kafka4.DescribeQuorumTest do
  @moduledoc """
  DescribeQuorum v0-v2 against apache/kafka:4.4.0 (combined broker +
  controller node; the broker forwards to the controller). Every response
  must decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  defp request(version) do
    struct(Module.concat([Kayrock.DescribeQuorum, :"V#{version}", Request]),
      topics: [%{topic_name: "__cluster_metadata", partitions: [%{partition_index: 0}]}]
    )
  end

  for version <- 0..2 do
    test "v#{version} describes the metadata quorum", %{kafka: kafka} do
      version = unquote(version)
      node_id = node_id()

      assert {response, <<>>} = raw_call(kafka, request(version))
      assert response.error_code == 0

      assert [%{topic_name: "__cluster_metadata", partitions: [partition]}] = response.topics
      assert %{partition_index: 0, error_code: 0, leader_id: ^node_id} = partition
      assert partition.leader_epoch >= 1
      assert partition.high_watermark > 0
      assert [%{replica_id: ^node_id} = voter] = partition.current_voters
      assert voter.log_end_offset > 0
      assert partition.observers == []

      if version >= 1 do
        assert voter.last_fetch_timestamp > 0
        assert voter.last_caught_up_timestamp > 0
      end

      if version >= 2 do
        assert is_binary(voter.replica_directory_id) and
                 byte_size(voter.replica_directory_id) == 16

        assert [%{node_id: ^node_id, listeners: [%{name: "CONTROLLER", port: 9093}]}] =
                 response.nodes
      end
    end
  end
end
