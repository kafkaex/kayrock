defmodule Kayrock.Kafka4.KIP848ConsumerGroupTest do
  @moduledoc """
  The KIP-848 ("next generation") consumer group protocol against
  `apache/kafka:4.4.0`, using the JSON-sourced versions:

    * ConsumerGroupHeartbeat v0-v1 (API 68): join with member epoch 0, get an
      assignment made of topic ids, heartbeat with the epoch, leave with -1;
    * Metadata v10-v13: resolve those topic ids back to topic names (and,
      v12+, look a topic up by id);
    * OffsetCommit v9-v10 / OffsetFetch v7-v10: commit and fetch offsets
      fenced by the member epoch (v10 by topic id);
    * ConsumerGroupDescribe v0-v1 (API 69);
    * FindCoordinator v4-v6 (batched).

  The consumer group protocol is enabled by default in Kafka 4.x, so the
  default single-node container is enough. Every request is sent over a raw
  `Kayrock.BrokerConnection` (a single node is coordinator and leader for
  everything) and every response must decode with no bytes left over.
  """
  use Kayrock.Kafka4Case

  import Kayrock.TestSupport, only: [unique_string: 0]

  # FENCED_MEMBER_EPOCH, STALE_MEMBER_EPOCH, UNKNOWN_MEMBER_ID
  @fenced_member_epoch 110
  @stale_member_epoch 113
  @unknown_member_id 25

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    {:ok, conn} = connect_broker(kafka)

    on_exit(fn ->
      if Process.alive?(conn), do: Kayrock.BrokerConnection.stop(conn)
    end)

    {:ok, client: client, conn: conn}
  end

  # Sends `request` on `conn` and returns the decoded response, asserting the
  # response decodes fully (no leftover bytes).
  defp call(conn, request) do
    request = %{
      request
      | correlation_id: rem(System.unique_integer([:positive]), 2_000_000_000),
        client_id: "kayrock-kip848"
    }

    :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
    {:ok, data} = Kayrock.BrokerConnection.recv(conn, 10_000)
    {response, rest} = Kayrock.Request.response_deserializer(request).(data)

    assert rest == <<>>,
           "#{inspect(request.__struct__)}: #{byte_size(rest)} bytes left over after decoding"

    assert response.correlation_id == request.correlation_id
    response
  end

  defp heartbeat(version, fields) do
    struct(
      Kayrock.ConsumerGroupHeartbeat.get_request_struct(version),
      Keyword.merge(
        [
          instance_id: nil,
          rack_id: nil,
          rebalance_timeout_ms: -1,
          subscribed_topic_names: nil,
          server_assignor: nil,
          topic_partitions: nil
        ],
        fields
      )
    )
  end

  # v0: the coordinator generates the member id; v1+: the client does (KIP-1082)
  defp member_id_for(0), do: ""
  defp member_id_for(_), do: "kayrock-#{Base.encode16(:crypto.strong_rand_bytes(8))}"

  # Locate the group coordinator (FindCoordinator v6, batched) and join
  # (member_epoch 0), then heartbeat until the member owns all `partitions`
  # partitions. Returns {member_id, epoch, assignment}.
  #
  # Like a real client, look the coordinator up first: on KRaft the
  # __consumer_offsets topic is only created by FindCoordinator, and until it
  # exists ConsumerGroupHeartbeat answers NOT_COORDINATOR.
  defp join(conn, version, group_id, topic, partitions) do
    find = %{
      Kayrock.FindCoordinator.get_request_struct(6)
      | key_type: 0,
        coordinator_keys: [group_id]
    }

    assert [%{key: ^group_id, error_code: 0, node_id: 1}] = await_coordinators(conn, find, 40)

    join =
      heartbeat(version,
        group_id: group_id,
        member_id: member_id_for(version),
        member_epoch: 0,
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: [topic],
        topic_partitions: []
      )

    response = call_retrying_coordinator(conn, join, 40)
    assert response.error_code == 0, "join v#{version}: #{inspect(response)}"
    assert response.member_epoch > 0
    assert response.heartbeat_interval_ms > 0
    assert is_binary(response.member_id) and response.member_id != ""

    if version >= 1, do: assert(response.member_id == join.member_id)

    await_assignment(conn, version, group_id, response, partitions, 50)
  end

  # COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR:
  # the group coordinator of a freshly started broker may still be loading.
  defp call_retrying_coordinator(conn, request, retries) do
    response = call(conn, request)

    if response.error_code in [14, 15, 16] and retries > 0 do
      Process.sleep(250)
      call_retrying_coordinator(conn, request, retries - 1)
    else
      response
    end
  end

  defp await_assignment(_conn, _version, _group_id, response, _partitions, 0),
    do: flunk("no full assignment received: #{inspect(response)}")

  defp await_assignment(conn, version, group_id, response, partitions, retries) do
    case response.assignment do
      %{topic_partitions: [%{partitions: assigned}] = tps} when length(assigned) == partitions ->
        {response.member_id, response.member_epoch, tps}

      _ ->
        Process.sleep(200)

        hb =
          call(
            conn,
            heartbeat(version,
              group_id: group_id,
              member_id: response.member_id,
              member_epoch: response.member_epoch
            )
          )

        assert hb.error_code == 0, "heartbeat v#{version}: #{inspect(hb)}"
        # Keep the latest assignment if this heartbeat did not carry one
        hb = if hb.assignment == nil, do: %{hb | assignment: response.assignment}, else: hb
        await_assignment(conn, version, group_id, hb, partitions, retries - 1)
    end
  end

  defp metadata_by_name(version, topic) do
    request = Kayrock.Metadata.get_request_struct(version)

    request = %{
      request
      | topics: [%{topic_id: nil, name: topic, tagged_fields: []}],
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: false
    }

    if version == 10,
      do: Map.put(request, :include_cluster_authorized_operations, false),
      else: request
  end

  defp metadata_by_id(version, topic_id) do
    %{
      Kayrock.Metadata.get_request_struct(version)
      | topics: [%{topic_id: topic_id, name: nil, tagged_fields: []}],
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: false
    }
  end

  defp topic_id_of(conn, topic) do
    %{topics: [%{name: ^topic, error_code: 0, topic_id: topic_id}]} =
      call(conn, metadata_by_name(12, topic))

    topic_id
  end

  describe "ConsumerGroupHeartbeat" do
    for version <- 0..1 do
      test "v#{version}: join (epoch 0), topic-id assignment, Metadata lookup, heartbeat, leave (-1)",
           %{client: client, conn: conn} do
        version = unquote(version)
        topic = create_kafka4_topic(client, partitions: 2)
        group_id = "kip848-#{unique_string()}"

        {member_id, epoch, [assigned]} = join(conn, version, group_id, topic, 2)
        assert byte_size(assigned.topic_id) == 16
        assert assigned.topic_id != <<0::128>>
        assert Enum.sort(assigned.partitions) == [0, 1]

        # Resolve the assignment's topic id to a name through Metadata v10+
        for md_version <- 10..Kayrock.Metadata.max_vsn() do
          response = call(conn, metadata_by_name(md_version, topic))
          assert [%{error_code: 0, name: ^topic, topic_id: topic_id}] = response.topics
          assert topic_id == assigned.topic_id, "Metadata v#{md_version}"
        end

        # v12+ also looks topics up by id (name null in the request)
        for md_version <- 12..Kayrock.Metadata.max_vsn() do
          response = call(conn, metadata_by_id(md_version, assigned.topic_id))

          assert [%{error_code: 0, name: ^topic, partitions: partitions}] = response.topics,
                 "Metadata v#{md_version} by id"

          assert length(partitions) == 2
          if md_version >= 13, do: assert(response.error_code == 0)
        end

        # Steady-state heartbeat carrying the owned partitions
        hb =
          call(
            conn,
            heartbeat(version,
              group_id: group_id,
              member_id: member_id,
              member_epoch: epoch,
              topic_partitions: [
                %{topic_id: assigned.topic_id, partitions: assigned.partitions}
              ]
            )
          )

        assert hb.error_code == 0, inspect(hb)
        assert hb.member_epoch == epoch
        assert hb.member_id == member_id

        # A stale epoch is fenced
        fenced =
          call(
            conn,
            heartbeat(version,
              group_id: group_id,
              member_id: member_id,
              member_epoch: epoch + 100
            )
          )

        assert fenced.error_code == @fenced_member_epoch

        # Leave with member_epoch -1
        leave =
          call(
            conn,
            heartbeat(version, group_id: group_id, member_id: member_id, member_epoch: -1)
          )

        assert leave.error_code == 0, inspect(leave)
        assert leave.member_epoch == -1
        assert leave.member_id == member_id
      end
    end
  end

  describe "ConsumerGroupDescribe" do
    test "v0-v1 describe a KIP-848 group and its member", %{client: client, conn: conn} do
      topic = create_kafka4_topic(client, partitions: 2)
      group_id = "kip848-#{unique_string()}"
      {member_id, epoch, [assigned]} = join(conn, 1, group_id, topic, 2)

      for version <- 0..Kayrock.ConsumerGroupDescribe.max_vsn() do
        request = %{
          Kayrock.ConsumerGroupDescribe.get_request_struct(version)
          | group_ids: [group_id, "kip848-missing-#{unique_string()}"],
            include_authorized_operations: true
        }

        assert %{groups: [group, missing]} = call(conn, request)

        assert group.error_code == 0, inspect(group)
        assert group.group_id == group_id
        assert group.group_state == "Stable"
        assert group.group_epoch >= epoch
        assert group.assignment_epoch >= epoch
        assert group.assignor_name in ["uniform", "range"]
        assert group.authorized_operations > 0

        assert [member] = group.members
        assert member.member_id == member_id
        assert member.member_epoch == epoch
        assert member.client_id == "kayrock-kip848"
        assert member.subscribed_topic_names == [topic]

        assert [%{topic_id: topic_id, topic_name: ^topic, partitions: partitions}] =
                 member.assignment.topic_partitions

        assert topic_id == assigned.topic_id
        assert Enum.sort(partitions) == [0, 1]
        assert %{topic_partitions: [%{topic_id: ^topic_id}]} = member.target_assignment

        # v1 adds member_type: 1 = consumer (KIP-848) member
        if version >= 1, do: assert(member.member_type == 1)

        # GROUP_ID_NOT_FOUND (69)
        assert missing.error_code == 69
      end

      call(conn, heartbeat(1, group_id: group_id, member_id: member_id, member_epoch: -1))
    end
  end

  describe "OffsetCommit / OffsetFetch with the member epoch" do
    test "OffsetCommit v9-v10 and OffsetFetch v7-v10 against a KIP-848 member",
         %{client: client, conn: conn} do
      topic = create_kafka4_topic(client, partitions: 1)
      group_id = "kip848-#{unique_string()}"
      {member_id, epoch, [assigned]} = join(conn, 1, group_id, topic, 1)
      topic_id = assigned.topic_id
      assert topic_id == topic_id_of(conn, topic)

      for version <- 9..Kayrock.OffsetCommit.max_vsn() do
        offset = 100 + version
        topic_entry = if version >= 10, do: %{topic_id: topic_id}, else: %{name: topic}

        commit = fn member_epoch ->
          %{
            Kayrock.OffsetCommit.get_request_struct(version)
            | group_id: group_id,
              generation_id_or_member_epoch: member_epoch,
              member_id: member_id,
              group_instance_id: nil,
              topics: [
                Map.put(topic_entry, :partitions, [
                  %{
                    partition_index: 0,
                    committed_offset: offset,
                    committed_leader_epoch: -1,
                    committed_metadata: "v#{version}"
                  }
                ])
              ]
          }
        end

        # A wrong member epoch is rejected (STALE_MEMBER_EPOCH)
        response = call(conn, commit.(epoch + 100))
        assert [%{partitions: [%{error_code: code}]}] = response.topics
        assert code == @stale_member_epoch, "OffsetCommit v#{version} stale epoch"

        response = call(conn, commit.(epoch))
        assert [%{partitions: [%{partition_index: 0, error_code: 0}]} = t] = response.topics
        if version >= 10, do: assert(t.topic_id == topic_id), else: assert(t.name == topic)

        # v7 (single group, no member epoch)
        v7 = %Kayrock.OffsetFetch.V7.Request{
          group_id: group_id,
          topics: [%{name: topic, partition_indexes: [0]}],
          require_stable: true
        }

        assert %{error_code: 0, topics: [%{partitions: [partition]}]} = call(conn, v7)
        assert {partition.committed_offset, partition.metadata} == {offset, "v#{version}"}

        # v8 (batched groups, no member epoch)
        v8 = %Kayrock.OffsetFetch.V8.Request{
          groups: [%{group_id: group_id, topics: [%{name: topic, partition_indexes: [0]}]}],
          require_stable: true
        }

        assert %{groups: [%{error_code: 0, topics: [%{partitions: [partition]}]}]} =
                 call(conn, v8)

        assert partition.committed_offset == offset

        # v9-v10 carry the member id / epoch; v10 by topic id
        for fetch_version <- 9..Kayrock.OffsetFetch.max_vsn() do
          fetch_topic =
            if fetch_version >= 10,
              do: %{topic_id: topic_id, partition_indexes: [0]},
              else: %{name: topic, partition_indexes: [0]}

          fetch = fn member_epoch ->
            %{
              Kayrock.OffsetFetch.get_request_struct(fetch_version)
              | groups: [
                  %{
                    group_id: group_id,
                    member_id: member_id,
                    member_epoch: member_epoch,
                    topics: [fetch_topic]
                  }
                ],
                require_stable: true
            }
          end

          assert %{groups: [group]} = call(conn, fetch.(epoch))
          assert group.error_code == 0, "OffsetFetch v#{fetch_version}: #{inspect(group)}"
          assert [%{partitions: [partition]} = t] = group.topics
          assert partition.error_code == 0
          assert partition.committed_offset == offset

          if fetch_version >= 10,
            do: assert(t.topic_id == topic_id),
            else: assert(t.name == topic)

          assert %{groups: [%{error_code: @stale_member_epoch}]} = call(conn, fetch.(epoch + 100))
        end
      end

      leave = call(conn, heartbeat(1, group_id: group_id, member_id: member_id, member_epoch: -1))
      assert leave.error_code == 0

      # After leaving, the member id is unknown to the group
      fetch = %Kayrock.OffsetFetch.V10.Request{
        groups: [
          %{
            group_id: group_id,
            member_id: member_id,
            member_epoch: epoch,
            topics: [%{topic_id: topic_id, partition_indexes: [0]}]
          }
        ],
        require_stable: false
      }

      assert %{groups: [%{error_code: @unknown_member_id}]} = call(conn, fetch)
    end
  end

  describe "FindCoordinator" do
    for version <- 4..6 do
      test "v#{version} batched group coordinator lookup", %{conn: conn} do
        version = unquote(version)
        keys = ["kip848-#{unique_string()}", "kip848-#{unique_string()}"]

        request = %{
          Kayrock.FindCoordinator.get_request_struct(version)
          | key_type: 0,
            coordinator_keys: keys
        }

        coordinators = await_coordinators(conn, request, 20)
        assert Enum.sort(Enum.map(coordinators, & &1.key)) == Enum.sort(keys)

        for coordinator <- coordinators do
          assert coordinator.error_code == 0, inspect(coordinator)
          assert coordinator.node_id == 1
          assert is_binary(coordinator.host) and coordinator.port > 0
        end
      end
    end
  end

  # The group coordinator may still be loading right after startup.
  defp await_coordinators(conn, request, retries) do
    %{coordinators: coordinators} = call(conn, request)

    if retries > 0 and Enum.any?(coordinators, &(&1.error_code != 0)) do
      Process.sleep(250)
      await_coordinators(conn, request, retries - 1)
    else
      coordinators
    end
  end
end
