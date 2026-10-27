defmodule Kayrock.Kafka4.StreamsGroupTest do
  @moduledoc """
  KIP-1071 streams groups against `apache/kafka:4.4.0`:
  StreamsGroupHeartbeat (88) v0-v1, StreamsGroupDescribe (89) v0-v1 and
  StreamsGroupTopologyDescriptionUpdate (93) v0 (KIP-1331).

  Streams groups are enabled by default in 4.4.0 (no feature flag needed).
  The broker is configured with:

    * `group.streams.initial.rebalance.delay.ms=0`, so a joining member gets
      its assignment on the next heartbeat instead of `ASSIGNMENT_DELAYED`;
    * `group.streams.topology.description.plugin.class` set to the
      `InMemoryTopologyDescriptionPlugin` shipped in the image, without which
      StreamsGroupTopologyDescriptionUpdate fails with UNSUPPORTED_VERSION
      and StreamsGroupDescribe v1 reports `NOT_STORED`.

  On a fresh single node the group coordinator is only started once
  `__consumer_offsets` exists, which a FindCoordinator request triggers;
  `setup_all` waits for that so the tests do not see NOT_COORDINATOR.

  Every response is checked to decode completely (no leftover bytes).
  """
  use Kayrock.Kafka4Case,
    container:
      Kayrock.Kafka4Container.new()
      |> Kayrock.Kafka4Container.with_broker_config(%{
        "group.streams.initial.rebalance.delay.ms" => 0,
        "group.streams.topology.description.plugin.class" =>
          "org.apache.kafka.server.streams.InMemoryTopologyDescriptionPlugin"
      })

  alias Kayrock.Kafka4.RawCall
  alias Kayrock.StreamsGroupDescribe
  alias Kayrock.StreamsGroupHeartbeat
  alias Kayrock.StreamsGroupTopologyDescriptionUpdate

  # COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR,
  # and UNKNOWN_SERVER_ERROR: a join right after CreateTopics can hit a 4.4.0
  # coordinator exception (NoSuchElementException in
  # EndpointToPartitionsManager) until the new topic is in the coordinator's
  # metadata image; a retry then succeeds
  @join_retriable [14, 15, 16, -1]
  @group_id_not_found 69

  setup_all %{kafka: kafka} do
    {:ok, conn} = connect_broker(kafka)
    :ok = wait_for_group_coordinator(conn)
    :ok = Kayrock.BrokerConnection.stop(conn)
  end

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 2)
    :ok = Kayrock.Client.stop(client)
    {:ok, conn} = connect_broker(kafka)
    {:ok, conn: conn, topic: topic, group: "streams-" <> topic}
  end

  test "the broker advertises the streams group APIs", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    broker = broker_api_versions(client)

    for {api, max} <- [
          streams_group_heartbeat: 1,
          streams_group_describe: 1,
          streams_group_topology_description_update: 0
        ],
        vsn <- 0..max do
      assert broker_supports?(broker, api, vsn), "#{api} v#{vsn}"
    end
  end

  for vsn <- 0..1 do
    test "StreamsGroupHeartbeat v#{vsn}: join, get an assignment, leave", ctx do
      vsn = unquote(vsn)
      %{conn: conn, topic: topic, group: group} = ctx

      joined = join!(conn, vsn, group, "member-v#{vsn}", topic)
      assert joined.error_code == 0, inspect(joined)
      assert joined.member_id == "member-v#{vsn}"
      assert joined.member_epoch >= 1
      assert joined.heartbeat_interval_ms > 0

      assigned = await_assignment(conn, vsn, group, joined)

      assert [%{subtopology_id: "0", partitions: partitions}] = assigned.active_tasks
      assert Enum.sort(partitions) == [0, 1]

      assert [%{user_endpoint: %{host: "localhost", port: 8080}} | _] =
               assigned.partitions_by_user_endpoint

      case vsn do
        0 ->
          assert is_integer(assigned.acceptable_recovery_lag_legacy)
          refute Map.has_key?(assigned, :topology_description_required)

        1 ->
          assert is_integer(assigned.acceptable_recovery_lag)
          assert assigned.topology_description_required in [0, 1]
          refute Map.has_key?(assigned, :acceptable_recovery_lag_legacy)
      end

      # steady-state heartbeat: every "unchanged" field null (null arrays,
      # null nullable structs); the broker answers with nulls too
      steady = heartbeat!(conn, vsn, group, assigned.member_id, assigned.member_epoch)
      assert steady.error_code == 0, inspect(steady)
      assert steady.member_epoch == assigned.member_epoch

      left = heartbeat!(conn, vsn, group, assigned.member_id, -1)
      assert left.error_code == 0
      assert left.member_epoch == -1
    end
  end

  test "StreamsGroupHeartbeat v1 for an unknown group with a non-zero epoch", %{conn: conn} do
    response =
      retry_join(fn ->
        heartbeat!(conn, 1, "no-such-streams-group-#{System.unique_integer([:positive])}", "m", 5)
      end)

    assert response.error_code == @group_id_not_found
    assert is_binary(response.error_message)
  end

  test "StreamsGroupTopologyDescriptionUpdate v0 and StreamsGroupDescribe v0-v1", ctx do
    %{conn: conn, topic: topic, group: group} = ctx

    joined = join!(conn, 1, group, "member-d", topic)
    assert joined.error_code == 0, inspect(joined)

    # a v1 heartbeat tells the member whether the broker wants a description
    assert joined.topology_description_required in [0, 1]

    update =
      RawCall.call!(conn, %StreamsGroupTopologyDescriptionUpdate.V0.Request{
        correlation_id: next_id(),
        client_id: "kayrock-kafka4",
        group_id: group,
        member_id: joined.member_id,
        topology_epoch: 0,
        topology_description: topology_description(topic)
      })

    assert %StreamsGroupTopologyDescriptionUpdate.V0.Response{error_code: 0, tagged_fields: []} =
             update

    # v1 with the topology description
    described =
      describe_until(conn, 1, group, fn group ->
        group.topology_description_status == 3 and group.members != []
      end)

    assert described.error_code == 0
    assert described.group_id == group
    assert is_binary(described.group_state) and described.group_state != ""
    assert is_binary(described.assignor_name)

    # topology.subtopologies and topology_description.subtopologies share a
    # field name but have different schemas
    assert %{epoch: 0, subtopologies: [%{subtopology_id: "0", source_topics: [^topic]}]} =
             described.topology

    assert described.topology_description == expected_topology_description(topic)

    assert [member] = described.members
    assert member.member_id == "member-d"
    assert member.process_id == "process-1"
    assert member.user_endpoint == %{host: "localhost", port: 8080, tagged_fields: []}
    assert member.client_tags == [%{key: "zone", value: "a", tagged_fields: []}]
    assert %{active_tasks: _, standby_tasks: _, warmup_tasks: _} = member.assignment
    assert %{active_tasks: _, standby_tasks: _, warmup_tasks: _} = member.target_assignment

    # v0 (no topology description fields)
    [v0] = describe!(conn, 0, [group]).groups
    assert v0.error_code == 0
    assert v0.topology == described.topology
    refute Map.has_key?(v0, :topology_description)
    refute Map.has_key?(v0, :assignor_name)

    # v1 without asking for the description
    [plain] = describe!(conn, 1, [group], include_topology_description: false).groups
    assert plain.topology_description == nil
    assert plain.topology_description_status == 0
  end

  for vsn <- 0..1 do
    test "StreamsGroupDescribe v#{vsn} for an unknown group", %{conn: conn} do
      [group] = describe!(conn, unquote(vsn), ["no-such-streams-group"]).groups
      assert group.error_code == @group_id_not_found
      assert group.group_id == "no-such-streams-group"
      assert group.topology == nil
      assert group.members == []
    end
  end

  ## helpers

  defp next_id, do: System.unique_integer([:positive, :monotonic])

  defp topology(topic) do
    %{
      epoch: 0,
      subtopologies: [
        %{
          subtopology_id: "0",
          source_topics: [topic],
          source_topic_regex: [],
          state_changelog_topics: [],
          repartition_sink_topics: [],
          repartition_source_topics: [],
          copartition_groups: []
        }
      ]
    }
  end

  defp topology_description(topic) do
    %{
      subtopologies: [
        %{
          subtopology_id: "0",
          nodes: [
            %{
              name: "KSTREAM-SOURCE-0000000000",
              node_type: 1,
              source_topics: [topic],
              sink_topic: nil,
              stores: [],
              successors: ["KSTREAM-SINK-0000000001"]
            },
            %{
              name: "KSTREAM-SINK-0000000001",
              node_type: 3,
              source_topics: [],
              sink_topic: topic <> "-out",
              stores: [],
              successors: []
            }
          ]
        }
      ],
      global_stores: []
    }
  end

  defp expected_topology_description(topic) do
    %{
      subtopologies: [
        %{
          subtopology_id: "0",
          nodes:
            Enum.map(
              topology_description(topic).subtopologies |> hd() |> Map.fetch!(:nodes),
              &Map.put(&1, :tagged_fields, [])
            ),
          tagged_fields: []
        }
      ],
      global_stores: [],
      tagged_fields: []
    }
  end

  defp heartbeat_request(vsn, group, member_id, epoch) do
    vsn
    |> StreamsGroupHeartbeat.get_request_struct()
    |> struct(
      correlation_id: next_id(),
      client_id: "kayrock-kafka4",
      group_id: group,
      member_id: member_id,
      member_epoch: epoch,
      endpoint_information_epoch: 0,
      rebalance_timeout_ms: -1,
      active_tasks: nil,
      standby_tasks: nil,
      warmup_tasks: nil,
      client_tags: nil,
      task_offsets: nil,
      task_end_offsets: nil,
      shutdown_application: false
    )
  end

  defp heartbeat!(conn, vsn, group, member_id, epoch) do
    RawCall.call!(conn, heartbeat_request(vsn, group, member_id, epoch))
  end

  defp join!(conn, vsn, group, member_id, topic) do
    request =
      vsn
      |> heartbeat_request(group, member_id, 0)
      |> struct(
        rebalance_timeout_ms: 30_000,
        topology: topology(topic),
        active_tasks: [],
        standby_tasks: [],
        warmup_tasks: [],
        process_id: "process-1",
        user_endpoint: %{host: "localhost", port: 8080},
        client_tags: [%{key: "zone", value: "a"}],
        task_offsets: [],
        task_end_offsets: []
      )

    retry_join(fn -> RawCall.call!(conn, %{request | correlation_id: next_id()}) end)
  end

  # The response that first carries an active-task assignment: the join
  # response itself, or a later heartbeat (which reports the tasks owned so
  # far; null task lists in a response mean "unchanged").
  defp await_assignment(conn, vsn, group, response, retries \\ 50)

  defp await_assignment(_conn, _vsn, _group, %{active_tasks: [_ | _]} = response, _retries),
    do: response

  defp await_assignment(_conn, _vsn, _group, response, 0),
    do: flunk("no assignment after repeated heartbeats: #{inspect(response)}")

  defp await_assignment(conn, vsn, group, last, retries) do
    Process.sleep(200)

    response =
      vsn
      |> heartbeat_request(group, last.member_id, last.member_epoch)
      |> struct(active_tasks: [], standby_tasks: [], warmup_tasks: [])
      |> then(&RawCall.call!(conn, &1))

    assert response.error_code == 0, inspect(response)
    await_assignment(conn, vsn, group, response, retries - 1)
  end

  defp describe!(conn, vsn, group_ids, opts \\ []) do
    request =
      vsn
      |> StreamsGroupDescribe.get_request_struct()
      |> struct(
        correlation_id: next_id(),
        client_id: "kayrock-kafka4",
        group_ids: group_ids,
        include_authorized_operations: true
      )

    request =
      if vsn >= 1,
        do: %{
          request
          | include_topology_description: Keyword.get(opts, :include_topology_description, true)
        },
        else: request

    RawCall.call!(conn, request)
  end

  defp describe_until(conn, vsn, group_id, done?, retries \\ 50) do
    [group] = describe!(conn, vsn, [group_id]).groups

    cond do
      done?.(group) -> group
      retries == 0 -> flunk("streams group not described as expected: #{inspect(group)}")
      true -> Process.sleep(200) && describe_until(conn, vsn, group_id, done?, retries - 1)
    end
  end

  defp wait_for_group_coordinator(conn, retries \\ 100) do
    request = %{
      Kayrock.FindCoordinator.get_request_struct(3)
      | correlation_id: next_id(),
        client_id: "kayrock-kafka4",
        key: "kayrock-streams-warmup",
        key_type: 0
    }

    case RawCall.call!(conn, request) do
      %{error_code: 0} ->
        :ok

      _ when retries > 0 ->
        Process.sleep(200)
        wait_for_group_coordinator(conn, retries - 1)

      other ->
        flunk("group coordinator never became available: #{inspect(other)}")
    end
  end

  # retries while the error is one of @join_retriable
  defp retry_join(fun, retries \\ 50) do
    response = fun.()

    if response.error_code in @join_retriable and retries > 0 do
      Process.sleep(200)
      retry_join(fun, retries - 1)
    else
      response
    end
  end
end
