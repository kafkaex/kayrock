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

    topic_config = %{
      topic: topic_name,
      num_partitions: 3,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: []
    }

    %{request | create_topic_requests: [topic_config], timeout: 1000}
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
      epoch: Keyword.get(opts, :epoch, 0),
      topics:
        Enum.map(partition_data, fn partition ->
          %{
            topic: Keyword.fetch!(partition, :topic),
            partitions: [
              %{
                partition: Keyword.get(partition, :partition, 0),
                fetch_offset: Keyword.get(partition, :fetch_offset, 0),
                max_bytes: Keyword.get(partition, :max_bytes, max_bytes),
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

  defp coordinator_key(request, 0, group_id), do: %{request | group_id: group_id}

  defp coordinator_key(request, _, group_id) do
    %{request | coordinator_key: group_id, coordinator_type: 0}
  end

  @doc """
  Creates a request to join a consumer group
  Uses min of api_version and max supported version
  """
  def join_group_request(member_data, api_version) do
    api_version = min(Kayrock.JoinGroup.max_vsn(), api_version)
    request = Kayrock.JoinGroup.get_request_struct(api_version)
    topics = Map.fetch!(member_data, :topics)

    %{
      request
      | group_id: Map.fetch!(member_data, :group_id),
        session_timeout: Map.get(member_data, :session_timeout, 10_000),
        member_id: Map.get(member_data, :member_id, ""),
        protocol_type: "consumer",
        group_protocols: [
          %{
            protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: topics},
            protocol_name: Map.get(member_data, :protocol_name, "assign")
          }
        ]
    }
    |> add_rebalance_timeout(api_version, member_data)
  end

  defp add_rebalance_timeout(request, 0, _), do: request

  defp add_rebalance_timeout(request, _, member_data) do
    %{
      request
      | rebalance_timeout: Map.get(member_data, :rebalance_timeout, 30_000)
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
        group_assignment: build_assignments(assignments)
    }
  end

  defp build_assignments(assignments) do
    Enum.map(assignments, fn assignment ->
      %{
        member_id: Map.fetch!(assignment, :member_id),
        member_assignment: %Kayrock.MemberAssignment{
          partition_assignments: [
            %Kayrock.MemberAssignment.PartitionAssignment{
              topic: Map.fetch!(assignment, :topic),
              partitions: Map.fetch!(assignment, :partitions)
            }
          ]
        }
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
    %{request | group_ids: group_ids}
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
    %{request | groups: group_ids}
  end
end
