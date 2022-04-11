defmodule Kayrock.MemberAssignment do
  @moduledoc """
  Code to serialize/deserialize Kafka consumer group member assignments

  See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-SyncGroupRequest
  """

  defstruct version: 0, partition_assignments: [], user_data: ""

  defmodule PartitionAssignment do
    @moduledoc "Represents partition assignments for a specific topic"
    defstruct topic: nil, partitions: []

    @type t :: %__MODULE__{
            topic: binary,
            partitions: [integer]
          }
  end

  @type t :: %__MODULE__{
          version: integer,
          partition_assignments: [PartitionAssignment.t()],
          user_data: binary
        }

  @spec serialize(t) :: iodata
  def serialize(%__MODULE__{
        version: version,
        partition_assignments: partition_assignments,
        user_data: user_data
      }) do
    [
      <<version::16-signed, length(partition_assignments)::32-signed>>,
      Enum.map(partition_assignments, &serialize_partition_assignment/1),
      Kayrock.Serialize.serialize(:bytes, user_data)
    ]
  end

  @spec deserialize(binary) :: {t, binary}
  def deserialize(<<>>), do: {%__MODULE__{}, <<>>}

  def deserialize(<<0::32-signed>>), do: {%__MODULE__{}, <<>>}

  def deserialize(<<0::32-signed, rest::bits>>), do: {%__MODULE__{}, rest}

  def deserialize(<<data_size::32-signed, data::size(data_size)-binary, rest::bits>>) do
    {deserialize_member_assignments(data), rest}
  end

  defp deserialize_member_assignments(
         <<version::16-signed, assignments_size::32-signed, rest::binary>>
       ) do
    {partition_assignments, user_data} = parse_assignments(assignments_size, rest, [])

    %__MODULE__{
      version: version,
      partition_assignments: partition_assignments,
      user_data: user_data
    }
  end

  defp parse_assignments(0, rest, assignments), do: {assignments, rest}

  defp parse_assignments(
         size,
         <<topic_len::16-signed, topic::size(topic_len)-binary, partition_len::32-signed,
           rest::binary>>,
         assignments
       ) do
    {partitions, rest} = parse_partitions(partition_len, rest, [])

    parse_assignments(size - 1, rest, [
      %PartitionAssignment{topic: topic, partitions: partitions} | assignments
    ])
  end

  defp parse_partitions(0, rest, partitions), do: {partitions, rest}

  defp parse_partitions(
         size,
         <<partition::32-signed, rest::binary>>,
         partitions
       ) do
    parse_partitions(size - 1, rest, [partition | partitions])
  end

  defp serialize_partition_assignment(%PartitionAssignment{topic: topic, partitions: partitions}) do
    [
      Kayrock.Serialize.serialize(:string, topic),
      Kayrock.Serialize.serialize_array(:int32, partitions)
    ]
  end
end
