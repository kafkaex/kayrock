defmodule Kayrock.Kafka4.ListGroupsTest do
  @moduledoc """
  ListGroups v4 (states filter, KIP-518) and v5 (types filter, KIP-848)
  against `apache/kafka:4.4.0`.

  A classic group is created with JoinGroup v9 / SyncGroup v5 and listed
  while Stable, then again after LeaveGroup v5 while Empty. Requests go over a
  raw `Kayrock.BrokerConnection` so each response can be checked to decode
  with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.TestSupport, only: [unique_string: 0]

  alias Kayrock.ListGroups

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    :ok = Kayrock.Client.stop(client)

    {:ok, conn} = connect_broker(kafka)
    on_exit(fn -> if Process.alive?(conn), do: Kayrock.BrokerConnection.stop(conn) end)

    group_id = "kafka4-list-#{unique_string()}"
    :ok = await_coordinator!(conn, group_id)
    member_id = join_and_sync!(conn, group_id, topic)
    {:ok, conn: conn, group_id: group_id, member_id: member_id}
  end

  test "ListGroups v5 filters by group type", %{conn: conn, group_id: group_id} do
    all = list!(conn, %ListGroups.V5.Request{})

    assert %{group_type: "classic", group_state: "Stable", protocol_type: "consumer"} =
             find(all, group_id)

    classic = list!(conn, %ListGroups.V5.Request{types_filter: ["classic"]})
    assert find(classic, group_id)
    assert Enum.all?(classic.groups, &(&1.group_type == "classic"))

    # Group types are matched case-insensitively by the broker.
    assert find(list!(conn, %ListGroups.V5.Request{types_filter: ["Classic"]}), group_id)

    consumer = list!(conn, %ListGroups.V5.Request{types_filter: ["consumer"]})
    refute find(consumer, group_id)
    assert Enum.all?(consumer.groups, &(&1.group_type == "consumer"))

    combined =
      list!(conn, %ListGroups.V5.Request{states_filter: ["Stable"], types_filter: ["classic"]})

    assert find(combined, group_id)
    assert Enum.all?(combined.groups, &(&1.group_state == "Stable"))

    refute find(
             list!(conn, %ListGroups.V5.Request{
               states_filter: ["Empty"],
               types_filter: ["classic"]
             }),
             group_id
           )
  end

  test "ListGroups v4 filters by group state",
       %{conn: conn, group_id: group_id, member_id: member_id} do
    assert %{group_state: "Stable"} = find(list!(conn, %ListGroups.V4.Request{}), group_id)
    assert find(list!(conn, %ListGroups.V4.Request{states_filter: ["Stable"]}), group_id)
    refute find(list!(conn, %ListGroups.V4.Request{states_filter: ["Empty"]}), group_id)

    left =
      call!(conn, %Kayrock.LeaveGroup.V5.Request{
        group_id: group_id,
        members: [%{member_id: member_id, group_instance_id: nil, reason: nil, tagged_fields: []}],
        tagged_fields: []
      })

    assert left.error_code == 0

    assert %{group_state: "Empty"} =
             find(list!(conn, %ListGroups.V4.Request{states_filter: ["Empty"]}), group_id)

    refute find(list!(conn, %ListGroups.V4.Request{states_filter: ["Stable"]}), group_id)

    assert %{group_state: "Empty", group_type: "classic"} =
             find(list!(conn, %ListGroups.V5.Request{types_filter: ["classic"]}), group_id)
  end

  defp list!(conn, request) do
    response = call!(conn, request)
    assert response.error_code == 0
    response
  end

  defp find(%{groups: groups}, group_id), do: Enum.find(groups, &(&1.group_id == group_id))

  defp join_and_sync!(conn, group_id, topic) do
    join = fn member_id ->
      call!(conn, %Kayrock.JoinGroup.V9.Request{
        group_id: group_id,
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 10_000,
        member_id: member_id,
        group_instance_id: nil,
        protocol_type: "consumer",
        protocols: [
          %{
            name: "range",
            metadata: %Kayrock.GroupProtocolMetadata{topics: [topic]},
            tagged_fields: []
          }
        ],
        reason: nil,
        tagged_fields: []
      })
    end

    # KIP-394: the first join returns MEMBER_ID_REQUIRED (79) with an id
    %{error_code: 79, member_id: member_id} = join.("")
    %{error_code: 0, generation_id: generation_id} = join.(member_id)

    synced =
      call!(conn, %Kayrock.SyncGroup.V5.Request{
        group_id: group_id,
        generation_id: generation_id,
        member_id: member_id,
        group_instance_id: nil,
        protocol_type: "consumer",
        protocol_name: "range",
        assignments: [%{member_id: member_id, assignment: <<>>, tagged_fields: []}],
        tagged_fields: []
      })

    assert synced.error_code == 0
    member_id
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

  defp call!(conn, request) do
    correlation_id = rem(System.unique_integer([:positive]), 0x7FFFFFFF)
    request = %{request | correlation_id: correlation_id, client_id: "kayrock"}
    :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
    {:ok, frame} = Kayrock.BrokerConnection.recv(conn, 15_000)
    {response, rest} = Kayrock.Request.response_deserializer(request).(frame)
    assert rest == <<>>, "#{inspect(request.__struct__)} left #{byte_size(rest)} bytes"
    assert response.correlation_id == correlation_id
    response
  end
end
