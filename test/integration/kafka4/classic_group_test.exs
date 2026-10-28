defmodule Kayrock.Kafka4.ClassicGroupTest do
  @moduledoc """
  The classic consumer-group protocol against `apache/kafka:4.4.0` at the
  JSON-generated versions: JoinGroup v7-v9, SyncGroup v5, Heartbeat v4,
  LeaveGroup v5, DescribeGroups v6 and DeleteGroups v3.

  Requests go over a raw `Kayrock.BrokerConnection` (the single KRaft node is
  every group's coordinator) so each response can be checked to decode with
  no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.TestSupport, only: [unique_string: 0]

  alias Kayrock.GroupProtocolMetadata
  alias Kayrock.MemberAssignment
  alias Kayrock.MemberAssignment.PartitionAssignment

  # GROUP_ID_NOT_FOUND (KIP-1043)
  @group_id_not_found 69
  @member_id_required 79

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 2)
    :ok = Kayrock.Client.stop(client)

    {:ok, conn} = connect_broker(kafka)
    on_exit(fn -> if Process.alive?(conn), do: Kayrock.BrokerConnection.stop(conn) end)
    {:ok, conn: conn, topic: topic}
  end

  for join_version <- 7..9 do
    test "JoinGroup v#{join_version} / SyncGroup v5 / Heartbeat v4 / DescribeGroups v6 / " <>
           "LeaveGroup v5 / DeleteGroups v3",
         %{conn: conn, topic: topic} do
      join_version = unquote(join_version)
      group_id = "kafka4-classic-#{unique_string()}"
      :ok = await_coordinator!(conn, group_id)

      # --- JoinGroup: KIP-394 first returns MEMBER_ID_REQUIRED with an id
      first = call!(conn, join_request(join_version, group_id, "", topic))
      assert first.error_code == @member_id_required
      assert first.member_id != ""
      assert first.protocol_type == nil
      assert first.protocol_name == nil
      if join_version >= 9, do: assert(first.skip_assignment == 0)

      joined = call!(conn, join_request(join_version, group_id, first.member_id, topic))
      member_id = first.member_id

      assert joined.error_code == 0
      assert joined.member_id == member_id
      assert joined.leader == member_id
      assert joined.generation_id >= 1
      # KIP-559: v7+ returns the protocol type
      assert joined.protocol_type == "consumer"
      assert joined.protocol_name == "range"
      if join_version >= 9, do: assert(joined.skip_assignment == 0)

      assert [%{member_id: ^member_id, group_instance_id: nil, metadata: metadata}] =
               joined.members

      assert is_binary(metadata) and byte_size(metadata) > 0

      # --- SyncGroup v5 with protocol type/name (validated by the broker)
      assignment = %MemberAssignment{
        version: 0,
        partition_assignments: [%PartitionAssignment{topic: topic, partitions: [0, 1]}],
        user_data: ""
      }

      synced =
        call!(conn, %Kayrock.SyncGroup.V5.Request{
          group_id: group_id,
          generation_id: joined.generation_id,
          member_id: member_id,
          group_instance_id: nil,
          protocol_type: "consumer",
          protocol_name: "range",
          assignments: [%{member_id: member_id, assignment: assignment, tagged_fields: []}],
          tagged_fields: []
        })

      assert synced.error_code == 0
      assert synced.protocol_type == "consumer"
      assert synced.protocol_name == "range"
      # Kayrock.MemberAssignment decodes partition (and topic) lists in
      # reverse wire order (pre-existing behaviour), so compare as sets.
      assert assigned_partitions(synced.assignment) == %{topic => MapSet.new([0, 1])}

      # --- Heartbeat v4 (already at its max) at the new generation
      heartbeat =
        call!(conn, %Kayrock.Heartbeat.V4.Request{
          group_id: group_id,
          generation_id: joined.generation_id,
          member_id: member_id,
          group_instance_id: nil,
          tagged_fields: []
        })

      assert heartbeat.error_code == 0

      # --- DescribeGroups v6: one known group, one unknown (KIP-1043)
      missing = "kafka4-missing-#{unique_string()}"

      described =
        call!(conn, %Kayrock.DescribeGroups.V6.Request{
          groups: [group_id, missing],
          include_authorized_operations: true,
          tagged_fields: []
        })

      assert [stable, not_found] = described.groups
      assert stable.group_id == group_id
      assert stable.error_code == 0
      assert stable.error_message == nil
      assert stable.group_state == "Stable"
      assert stable.protocol_type == "consumer"
      assert stable.protocol_data == "range"
      assert stable.authorized_operations != -2_147_483_648

      assert [%{member_id: ^member_id, member_assignment: member_assignment}] = stable.members

      assert assigned_partitions(MemberAssignment.deserialize_content(member_assignment)) ==
               %{topic => MapSet.new([0, 1])}

      assert not_found.group_id == missing
      assert not_found.error_code == @group_id_not_found
      assert is_binary(not_found.error_message)

      # --- LeaveGroup v5 with a reason (KIP-800)
      left =
        call!(conn, %Kayrock.LeaveGroup.V5.Request{
          group_id: group_id,
          members: [
            %{
              member_id: member_id,
              group_instance_id: nil,
              reason: "kayrock kafka4 test",
              tagged_fields: []
            }
          ],
          tagged_fields: []
        })

      assert left.error_code == 0
      assert [%{member_id: ^member_id, group_instance_id: nil, error_code: 0}] = left.members

      # --- DeleteGroups v3: the now-empty group and an unknown one
      deleted =
        call!(conn, %Kayrock.DeleteGroups.V3.Request{
          groups_names: [group_id, missing],
          tagged_fields: []
        })

      results = Map.new(deleted.results, &{&1.group_id, &1})
      assert %{error_code: 0, error_message: nil} = results[group_id]
      assert results[missing].error_code == @group_id_not_found
    end
  end

  test "JoinGroup v8+ accepts a static member (group.instance.id) with a reason",
       %{conn: conn, topic: topic} do
    group_id = "kafka4-static-#{unique_string()}"
    :ok = await_coordinator!(conn, group_id)

    request = %{
      join_request(9, group_id, "", topic)
      | group_instance_id: "instance-1",
        reason: "static join"
    }

    # Static members get a member id straight away (no MEMBER_ID_REQUIRED).
    joined = call!(conn, request)
    assert joined.error_code == 0
    assert joined.leader == joined.member_id

    assert [%{member_id: member_id, group_instance_id: "instance-1"}] = joined.members
    assert member_id == joined.member_id

    left =
      call!(conn, %Kayrock.LeaveGroup.V5.Request{
        group_id: group_id,
        members: [
          %{member_id: "", group_instance_id: "instance-1", reason: nil, tagged_fields: []}
        ],
        tagged_fields: []
      })

    assert left.error_code == 0
    assert [%{group_instance_id: "instance-1", error_code: 0}] = left.members
  end

  defp assigned_partitions(%MemberAssignment{version: 0, partition_assignments: assignments}) do
    Map.new(assignments, &{&1.topic, MapSet.new(&1.partitions)})
  end

  defp join_request(version, group_id, member_id, topic) do
    request = Kayrock.JoinGroup.get_request_struct(version)

    request = %{
      request
      | group_id: group_id,
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 10_000,
        member_id: member_id,
        group_instance_id: nil,
        protocol_type: "consumer",
        protocols: [
          %{
            name: "range",
            metadata: %GroupProtocolMetadata{version: 0, topics: [topic], user_data: ""},
            tagged_fields: []
          }
        ],
        tagged_fields: []
    }

    if version >= 8, do: %{request | reason: nil}, else: request
  end

  # A fresh broker has no __consumer_offsets topic yet; FindCoordinator
  # creates it. Poll until the coordinator for `group_id` is available.
  defp await_coordinator!(conn, group_id, attempts \\ 60) do
    response =
      call!(conn, %Kayrock.FindCoordinator.V3.Request{
        key: group_id,
        key_type: 0,
        tagged_fields: []
      })

    cond do
      response.error_code == 0 ->
        :ok

      attempts > 1 and response.error_code in [14, 15, 16] ->
        Process.sleep(250)
        await_coordinator!(conn, group_id, attempts - 1)

      true ->
        flunk("FindCoordinator #{group_id}: #{inspect(response)}")
    end
  end

  # Sends `request` on the raw connection and decodes the response, asserting
  # the whole frame is consumed.
  defp call!(conn, request) do
    correlation_id = rem(System.unique_integer([:positive]), 0x7FFFFFFF)
    request = %{request | correlation_id: correlation_id, client_id: "kayrock"}
    :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
    {:ok, frame} = Kayrock.BrokerConnection.recv(conn, 15_000)
    {response, rest} = Kayrock.Request.response_deserializer(request).(frame)
    assert rest == <<>>, "#{inspect(request.__struct__)} left #{byte_size(rest)} bytes"
    assert response.correlation_id == request.correlation_id
    response
  end
end
