defmodule Kayrock.RequestFactory do
  @moduledoc """
  Documentation for Kayrock.RequestFactory.
  """

  @doc """
  Creates a request to create a topic
  Uses min of api_version and max supported version
  """
  def create_topic_request(topic_name, api_version) do
    api_version = min(Kayrock.CreateTopics.max_vsn(), api_version)
    request = Kayrock.CreateTopics.get_request_struct(api_version)

    topic_config =
      if api_version >= 5 do
        %{
          name: topic_name,
          num_partitions: 3,
          replication_factor: 1,
          assignments: [],
          configs: [],
          tagged_fields: []
        }
      else
        %{
          name: topic_name,
          num_partitions: 3,
          replication_factor: 1,
          assignments: [],
          configs: []
        }
      end

    request_with_topics = %{request | topics: [topic_config], timeout_ms: 1000}

    cond do
      api_version >= 5 ->
        %{request_with_topics | tagged_fields: [], validate_only: false}

      api_version >= 1 ->
        %{request_with_topics | validate_only: false}

      true ->
        request_with_topics
    end
  end

  @doc """
  Creates a request to produce a message to a topic
  Uses min of api_version and max supported version
  """
  def produce_messages_request(topic_name, data, ack, api_version) do
    api_version = min(Kayrock.Produce.max_vsn(), api_version)
    request = Kayrock.Produce.get_request_struct(api_version)

    topic_data = [
      %{
        topic: topic_name,
        data:
          Enum.map(data, fn datum ->
            %{
              partition: Keyword.get(datum, :partition, 0),
              record_set: Keyword.get(datum, :record_set, [])
            }
          end)
      }
    ]

    %{request | topic_data: topic_data, acks: ack, timeout: 1000}
  end

  @doc """
  Creates a request to fetch messages from a topic
  Uses min of api_version and max supported version
  """
  def fetch_messages_request(partition_data, opts, api_version) do
    api_version = min(Kayrock.Fetch.max_vsn(), api_version)
    request = Kayrock.Fetch.get_request_struct(api_version)
    max_bytes = Keyword.get(opts, :max_bytes, 1_000_000)

    request_date = %{
      replica_id: -1,
      max_wait_time: Keyword.get(opts, :max_wait_time, 1000),
      min_bytes: Keyword.get(opts, :min_bytes, 0),
      max_bytes: Keyword.get(opts, :max_bytes, max_bytes),
      isolation_level: Keyword.get(opts, :isolation_level, 1),
      session_id: Keyword.get(opts, :session_id, 0),
      session_epoch: Keyword.get(opts, :session_epoch, -1),
      epoch: Keyword.get(opts, :epoch, 0),
      topics:
        Enum.map(partition_data, fn partition ->
          %{
            topic: Keyword.fetch!(partition, :topic),
            partitions: [
              %{
                partition: Keyword.get(partition, :partition, 0),
                fetch_offset: Keyword.get(partition, :fetch_offset, 0),
                partition_max_bytes: Keyword.get(partition, :max_bytes, max_bytes),
                log_start_offset: Keyword.get(partition, :log_start_offset, 0)
              }
            ]
          }
        end)
    }

    %{Map.merge(request, request_date) | replica_id: -1}
  end

  @doc """
  Creates a request to join a consumer group
  Uses min of api_version and max supported version
  """
  def list_consumer_groups_request(api_version) do
    api_version = min(Kayrock.ListGroups.max_vsn(), api_version)
    Kayrock.ListGroups.get_request_struct(api_version)
  end

  @doc """
  Create a request to find coordinator for a consumer group
  Uses min of api_version and max supported version
  """
  def find_coordinator_request(group_id, api_version) do
    api_version = min(Kayrock.FindCoordinator.max_vsn(), api_version)
    request = Kayrock.FindCoordinator.get_request_struct(api_version)
    coordinator_key(request, api_version, group_id)
  end

  defp coordinator_key(request, 0, group_id), do: %{request | key: group_id}

  defp coordinator_key(request, _, group_id) do
    %{request | key: group_id, key_type: 0}
  end

  @doc """
  Creates a request to join a consumer group
  Uses min of api_version and max supported version
  """
  def join_group_request(member_data, api_version) do
    api_version = min(Kayrock.JoinGroup.max_vsn(), api_version)
    request = Kayrock.JoinGroup.get_request_struct(api_version)
    topics = Map.fetch!(member_data, :topics)

    metadata = %Kayrock.GroupProtocolMetadata{topics: topics}

    %{
      request
      | group_id: Map.fetch!(member_data, :group_id),
        session_timeout_ms: Map.get(member_data, :session_timeout, 10_000),
        member_id: Map.get(member_data, :member_id, ""),
        protocol_type: "consumer",
        protocols: [
          %{
            metadata: metadata,
            name: Map.get(member_data, :protocol_name, "assign")
          }
        ]
    }
    |> add_rebalance_timeout(api_version, member_data)
  end

  defp add_rebalance_timeout(request, 0, _), do: request

  defp add_rebalance_timeout(request, _, member_data) do
    %{
      request
      | rebalance_timeout_ms: Map.get(member_data, :rebalance_timeout, 30_000)
    }
  end

  @doc """
  Creates a request to sync a consumer group
  Uses min of api_version and max supported version
  """
  def sync_group_request(group_id, member_id, assignments, api_version) do
    api_version = min(Kayrock.SyncGroup.max_vsn(), api_version)
    request = Kayrock.SyncGroup.get_request_struct(api_version)

    %{
      request
      | group_id: group_id,
        member_id: member_id,
        generation_id: 1,
        assignments: build_assignments(assignments)
    }
  end

  defp build_assignments(assignments) do
    Enum.map(assignments, fn assignment ->
      member_assignment = %Kayrock.MemberAssignment{
        partition_assignments: [
          %Kayrock.MemberAssignment.PartitionAssignment{
            topic: Map.fetch!(assignment, :topic),
            partitions: Map.fetch!(assignment, :partitions)
          }
        ],
        version: 0,
        user_data: ""
      }

      %{
        member_id: Map.fetch!(assignment, :member_id),
        assignment: member_assignment
      }
    end)
  end

  @doc """
  Creates a request to describe a consumer groups
  Uses min of api_version and max supported version
  """
  def describe_groups_request(group_ids, api_version) do
    api_version = min(Kayrock.DescribeGroups.max_vsn(), api_version)
    request = Kayrock.DescribeGroups.get_request_struct(api_version)
    %{request | groups: group_ids}
  end

  @doc """
  Creates a request to leave a consumer group
  Uses min of api_version and max supported version
  """
  def leave_group_request(group_id, member_id, api_version) do
    api_version = min(Kayrock.LeaveGroup.max_vsn(), api_version)
    request = Kayrock.LeaveGroup.get_request_struct(api_version)
    %{request | group_id: group_id, member_id: member_id}
  end

  @doc """
  Creates a request to delete a group
  Uses min of api_version and max supported version
  """
  def delete_groups_request(group_ids, api_version) do
    api_version = min(Kayrock.DeleteGroups.max_vsn(), api_version)
    request = Kayrock.DeleteGroups.get_request_struct(api_version)
    %{request | groups_names: group_ids}
  end

  @doc """
  Creates a request to list offsets for a topic/partition
  Uses min of api_version and max supported version
  """
  def list_offsets_request(topic_name, partitions, api_version) do
    api_version = min(Kayrock.ListOffsets.max_vsn(), api_version)
    request = Kayrock.ListOffsets.get_request_struct(api_version)

    topics = [
      %{
        topic: topic_name,
        partitions:
          Enum.map(partitions, fn partition ->
            build_list_offset_partition(partition, api_version)
          end)
      }
    ]

    %{request | replica_id: -1, topics: topics}
    |> maybe_add_isolation_level(api_version)
  end

  defp build_list_offset_partition(partition, api_version) when api_version >= 1 do
    %{
      partition: Keyword.get(partition, :partition, 0),
      timestamp: Keyword.get(partition, :timestamp, -1),
      current_leader_epoch: Keyword.get(partition, :current_leader_epoch, -1)
    }
  end

  defp build_list_offset_partition(partition, _api_version) do
    %{
      partition: Keyword.get(partition, :partition, 0),
      timestamp: Keyword.get(partition, :timestamp, -1),
      max_num_offsets: Keyword.get(partition, :max_num_offsets, 1)
    }
  end

  defp maybe_add_isolation_level(request, api_version) when api_version >= 2 do
    %{request | isolation_level: 0}
  end

  defp maybe_add_isolation_level(request, _), do: request

  @doc """
  Creates a request to commit offsets
  Uses min of api_version and max supported version
  """
  def offset_commit_request(group_id, topics_data, api_version, opts \\ []) do
    api_version = min(Kayrock.OffsetCommit.max_vsn(), api_version)
    request = Kayrock.OffsetCommit.get_request_struct(api_version)

    topics =
      Enum.map(topics_data, fn topic_data ->
        %{
          name: Keyword.fetch!(topic_data, :topic),
          partitions:
            Enum.map(Keyword.get(topic_data, :partitions, []), fn partition ->
              %{
                partition_index: Keyword.get(partition, :partition, 0),
                committed_offset: Keyword.fetch!(partition, :offset),
                committed_metadata: Keyword.get(partition, :metadata, "")
              }
            end)
        }
      end)

    base_request = %{request | group_id: group_id, topics: topics}

    base_request
    |> maybe_add_generation_member(api_version, opts)
    |> maybe_add_retention_time(api_version, opts)
  end

  defp maybe_add_generation_member(request, api_version, opts) when api_version >= 1 do
    %{
      request
      | generation_id: Keyword.get(opts, :generation_id, -1),
        member_id: Keyword.get(opts, :member_id, "")
    }
  end

  defp maybe_add_generation_member(request, _, _), do: request

  defp maybe_add_retention_time(request, api_version, opts) when api_version >= 2 do
    %{request | retention_time_ms: Keyword.get(opts, :retention_time_ms, -1)}
  end

  defp maybe_add_retention_time(request, _, _), do: request

  @doc """
  Creates a request to fetch committed offsets
  Uses min of api_version and max supported version
  """
  def offset_fetch_request(group_id, topics_data, api_version) do
    api_version = min(Kayrock.OffsetFetch.max_vsn(), api_version)
    request = Kayrock.OffsetFetch.get_request_struct(api_version)

    topics =
      Enum.map(topics_data, fn topic_data ->
        %{
          topic: Keyword.fetch!(topic_data, :topic),
          partitions:
            Enum.map(Keyword.get(topic_data, :partitions, []), fn partition ->
              %{partition: Keyword.get(partition, :partition, 0)}
            end)
        }
      end)

    %{request | group_id: group_id, topics: topics}
  end

  @doc """
  Creates a heartbeat request for a consumer group
  Uses min of api_version and max supported version
  """
  def heartbeat_request(group_id, member_id, generation_id, api_version) do
    api_version = min(Kayrock.Heartbeat.max_vsn(), api_version)
    request = Kayrock.Heartbeat.get_request_struct(api_version)

    %{
      request
      | group_id: group_id,
        member_id: member_id,
        generation_id: generation_id
    }
  end

  @doc """
  Creates a metadata request for topics
  Uses min of api_version and max supported version
  """
  def metadata_request(topics, api_version) do
    api_version = min(Kayrock.Metadata.max_vsn(), api_version)
    request = Kayrock.Metadata.get_request_struct(api_version)

    topics_list =
      case topics do
        nil -> nil
        [] -> []
        list -> Enum.map(list, fn t -> %{name: t} end)
      end

    base = %{request | topics: topics_list}

    if api_version >= 4 do
      %{base | allow_auto_topic_creation: false}
    else
      base
    end
  end

  @doc """
  Creates an API versions request
  Uses min of api_version and max supported version
  """
  def api_versions_request(api_version) do
    api_version = min(Kayrock.ApiVersions.max_vsn(), api_version)
    Kayrock.ApiVersions.get_request_struct(api_version)
  end
end
