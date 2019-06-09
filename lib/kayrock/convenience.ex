defmodule Kayrock.Convenience do
  @moduledoc """
  Convenience functions for working with Kayrock / Kafka API data
  """

  alias Kayrock.ErrorCode

  def topic_exists?(pid, topic) when is_pid(pid) do
    {:ok, [topic]} = Kayrock.topics_metadata(pid, [topic])

    topic[:error_code] != ErrorCode.unknown_topic()
  end

  def partition_last_offset(client_pid, topic, partition) do
    %{^partition => offset} = partitions_last_offset(client_pid, topic, [partition])
    offset
  end

  @doc """
  Returns a map of partition to offset for partitions on the same leader

  All partitions must be on the same leader or you will get -1 for the offset
  """
  def partitions_last_offset(client_pid, topic, partitions) do
    [first_partition | _] = partitions

    partition_requests =
      for partition <- partitions do
        %{partition: partition, timestamp: -1}
      end

    request = %Kayrock.ListOffsets.V1.Request{
      replica_id: -1,
      topics: [%{topic: topic, partitions: partition_requests}]
    }

    {:ok, resp} =
      Kayrock.client_call(client_pid, request, {:topic_partition, topic, first_partition})

    [topic_resp] = resp.responses

    Enum.into(topic_resp.partition_responses, %{}, fn %{partition: partition, offset: offset} ->
      {partition, offset}
    end)
  end

  def topic_last_offsets(client_pid, topic) do
    partition_leaders = get_partition_leaders(client_pid, topic)

    partitions_by_node =
      Enum.reduce(partition_leaders, %{}, fn {partition, node}, acc ->
        Map.update(acc, node, [partition], fn existing_partitions ->
          [partition | existing_partitions]
        end)
      end)

    Enum.reduce(partitions_by_node, %{}, fn {_, partitions}, acc ->
      Map.merge(acc, partitions_last_offset(client_pid, topic, partitions))
    end)
  end

  def get_partition_leaders(client_pid, topic) do
    {:ok, [metadata]} = Kayrock.topics_metadata(client_pid, [topic])

    Enum.into(metadata.partition_metadata, %{}, fn partition_metadata ->
      {partition_metadata.partition, partition_metadata.leader}
    end)
  end
end
