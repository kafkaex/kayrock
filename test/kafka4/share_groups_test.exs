defmodule Kayrock.Kafka4.ShareGroupsTest do
  @moduledoc """
  KIP-932 share groups ("queues") against `apache/kafka:4.4.0`.

  No broker overrides are needed: formatting 4.4.0 storage for its latest
  release finalizes `share.version=2` and `group.version=1`
  (`kafka-features.sh version-mapping`), and the `share` group type is in the
  default `group.coordinator.rebalance.protocols`, so the default
  `Kayrock.Kafka4Container` serves share groups. (Pinning
  `with_feature("share.version", 1)` would downgrade the feature.) Share groups
  start from the latest offset by default, so the test sets the group config
  `share.auto.offset.reset=earliest` first.

  One flow exercises every new request version:

    1. produce records, set the group's `share.auto.offset.reset=earliest`
       (IncrementalAlterConfigs, resource type GROUP);
    2. ShareGroupHeartbeat v1 until the member is assigned the partition;
    3. ShareFetch v1 / ShareAcknowledge v1, then ShareFetch v2 /
       ShareAcknowledge v2, accepting every acquired record;
    4. ShareGroupDescribe v1 and DescribeShareGroupOffsets v0/v1;
    5. leave the group (heartbeat epoch -1), then AlterShareGroupOffsets v0
       and DeleteShareGroupOffsets v0 (both need an empty group).

  Every request goes over a raw `Kayrock.BrokerConnection` and every
  response must deserialize with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.RequestFactory, only: [produce_messages_request: 4]
  import Kayrock.TestSupport, only: [unique_string: 0]

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  @record_count 6
  @group_resource_type 32
  @accept 1

  test "heartbeat, fetch, acknowledge, describe and manage offsets of a share group",
       %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 1)
    group_id = "share-group-#{unique_string()}"
    member_id = Base.url_encode64(:crypto.strong_rand_bytes(16), padding: false)

    # one batch per record so acquisition can stop at record boundaries
    for i <- 1..@record_count, do: produce(client, topic, "m#{i}")

    {:ok, conn} = connect_broker(kafka)
    :ok = wait_for_coordinator(conn, group_id)
    :ok = set_auto_offset_reset_earliest(conn, group_id)

    # -- ShareGroupHeartbeat v1 -------------------------------------------
    {member_epoch, topic_id} = join_share_group(conn, group_id, member_id, topic)
    assert byte_size(topic_id) == 16

    # -- ShareFetch / ShareAcknowledge v1 then v2 -------------------------
    {offsets, session_epoch} =
      Enum.reduce(1..2, {[], 0}, fn version, {seen, epoch} ->
        {fetched, epoch} =
          fetch_until_records(conn, version, group_id, member_id, topic_id, epoch)

        acknowledge(conn, version, group_id, member_id, topic_id, epoch, fetched)
        {seen ++ fetched, epoch + 1}
      end)

    # fetch whatever the two rounds left, acknowledging it in the fetch itself
    offsets = drain(conn, group_id, member_id, topic_id, session_epoch, offsets)

    assert offsets |> Enum.map(&elem(&1, 0)) |> Enum.sort() ==
             Enum.to_list(0..(@record_count - 1))

    # offset N holds "m{N+1}"
    for {offset, value} <- offsets, do: assert(value == "m#{offset + 1}")

    # -- ShareGroupDescribe v1 ---------------------------------------------
    describe = %Kayrock.ShareGroupDescribe.V1.Request{
      group_ids: [group_id],
      include_authorized_operations: true
    }

    %{groups: [group]} = call(conn, describe)
    assert group.error_code == 0
    assert group.group_id == group_id
    assert group.group_state == "Stable"
    assert group.assignor_name == "simple"
    assert [member] = group.members
    assert member.member_id == member_id
    assert member.member_epoch == member_epoch
    assert member.subscribed_topic_names == [topic]

    assert [%{topic_id: ^topic_id, topic_name: ^topic, partitions: [0]}] =
             member.assignment.topic_partitions

    # -- DescribeShareGroupOffsets v0 / v1 -----------------------------------
    # Share-partition state is persisted asynchronously. The broker may keep
    # the persisted start offset behind fully acknowledged batches; v1's lag
    # (end offset - start offset - delivery-complete records, KIP-1226)
    # reaching 0 proves every record was acknowledged.
    %{start_offset: start_offset} =
      wait_for_offsets(conn, 1, group_id, topic, topic_id, &(&1.lag == 0))

    assert start_offset in 0..@record_count

    assert %{start_offset: ^start_offset} =
             wait_for_offsets(
               conn,
               0,
               group_id,
               topic,
               topic_id,
               &(&1.start_offset == start_offset)
             )

    refute Map.has_key?(
             wait_for_offsets(conn, 0, group_id, topic, topic_id, fn _ -> true end),
             :lag
           )

    # all topics (null topics array)
    request = %Kayrock.DescribeShareGroupOffsets.V1.Request{
      groups: [%{group_id: group_id, topics: nil}]
    }

    assert %{groups: [%{error_code: 0, topics: [%{topic_name: ^topic}]}]} = call(conn, request)

    # -- leave the group ---------------------------------------------------
    leave = share_heartbeat_request(group_id, member_id, -1, nil)
    assert %{error_code: 0, member_epoch: -1} = call(conn, leave)

    # -- AlterShareGroupOffsets v0 -----------------------------------------
    alter = %Kayrock.AlterShareGroupOffsets.V0.Request{
      group_id: group_id,
      topics: [%{topic_name: topic, partitions: [%{partition_index: 0, start_offset: 2}]}]
    }

    response = with_group_empty(fn -> call(conn, alter) end)
    assert response.error_code == 0

    assert [%{topic_name: ^topic, topic_id: ^topic_id, partitions: [partition]}] =
             response.responses

    assert %{partition_index: 0, error_code: 0} = partition

    assert %{start_offset: 2, lag: lag} =
             wait_for_offsets(conn, 1, group_id, topic, topic_id, &(&1.start_offset == 2))

    assert lag == @record_count - 2

    # -- DeleteShareGroupOffsets v0 ----------------------------------------
    delete = %Kayrock.DeleteShareGroupOffsets.V0.Request{
      group_id: group_id,
      topics: [%{topic_name: topic}]
    }

    response = call(conn, delete)
    assert response.error_code == 0

    assert [%{topic_name: ^topic, topic_id: ^topic_id, error_code: 0}] = response.responses

    describe_all = %Kayrock.DescribeShareGroupOffsets.V1.Request{
      groups: [%{group_id: group_id, topics: nil}]
    }

    assert %{groups: [%{error_code: 0, topics: []}]} = call(conn, describe_all)
  end

  test "describing an unknown share group reports GROUP_ID_NOT_FOUND", %{kafka: kafka} do
    {:ok, conn} = connect_broker(kafka)
    group_id = "missing-share-group-#{unique_string()}"
    :ok = wait_for_coordinator(conn, group_id)

    request = %Kayrock.ShareGroupDescribe.V1.Request{
      group_ids: [group_id],
      include_authorized_operations: false
    }

    # GROUP_ID_NOT_FOUND
    assert %{groups: [%{group_id: ^group_id, error_code: 69}]} = call(conn, request)
  end

  # -- share group helpers ---------------------------------------------------

  defp share_heartbeat_request(group_id, member_id, member_epoch, topics) do
    %Kayrock.ShareGroupHeartbeat.V1.Request{
      group_id: group_id,
      member_id: member_id,
      member_epoch: member_epoch,
      rack_id: nil,
      subscribed_topic_names: topics
    }
  end

  # Heartbeats until the member owns partition 0 of `topic`; returns the
  # member epoch and the topic id from the assignment.
  defp join_share_group(conn, group_id, member_id, topic) do
    response = call(conn, share_heartbeat_request(group_id, member_id, 0, [topic]))
    assert response.error_code == 0, inspect(response)
    assert response.member_id == member_id
    assert response.heartbeat_interval_ms > 0
    await_assignment(conn, group_id, member_id, response, 50)
  end

  defp await_assignment(
         _conn,
         _group_id,
         _member_id,
         %{member_epoch: epoch, assignment: %{topic_partitions: [%{partitions: [0]} = tp]}},
         _retries
       ),
       do: {epoch, tp.topic_id}

  defp await_assignment(conn, group_id, member_id, last, retries) when retries > 0 do
    Process.sleep(200)
    response = call(conn, share_heartbeat_request(group_id, member_id, last.member_epoch, nil))
    assert response.error_code == 0, inspect(response)
    await_assignment(conn, group_id, member_id, response, retries - 1)
  end

  defp share_fetch_request(1, group_id, member_id, topic_id, epoch, acks) do
    %Kayrock.ShareFetch.V1.Request{
      group_id: group_id,
      member_id: member_id,
      share_session_epoch: epoch,
      max_wait_ms: 500,
      min_bytes: 1,
      max_bytes: 1_048_576,
      max_records: 2,
      batch_size: 2,
      topics: [
        %{
          topic_id: topic_id,
          partitions: [%{partition_index: 0, acknowledgement_batches: acks}]
        }
      ],
      forgotten_topics_data: []
    }
  end

  defp share_fetch_request(2, group_id, member_id, topic_id, epoch, acks) do
    request = share_fetch_request(1, group_id, member_id, topic_id, epoch, acks)

    request
    |> Map.from_struct()
    |> then(&struct(Kayrock.ShareFetch.V2.Request, &1))
    # record-limit mode: acquire at most max_records records
    |> Map.merge(%{share_acquire_mode: 1, is_renew_ack: false})
  end

  # Fetches (opening the session at epoch 0 or continuing it) until records
  # are acquired; returns [{offset, value}] and the next session epoch.
  defp fetch_until_records(conn, version, group_id, member_id, topic_id, epoch, retries \\ 20) do
    request = share_fetch_request(version, group_id, member_id, topic_id, epoch, [])
    response = call(conn, request)
    assert response.error_code == 0, inspect(response)
    assert response.acquisition_lock_timeout_ms > 0

    assert [%{topic_id: ^topic_id, partitions: [partition]}] = response.responses
    assert partition.partition_index == 0
    assert partition.error_code == 0, inspect(partition)
    assert partition.acknowledge_error_code == 0
    # current_leader is only filled in on leadership errors (zeroed otherwise)
    assert %{leader_id: _, leader_epoch: _} = partition.current_leader

    records = acquired(partition)

    cond do
      records != [] ->
        if version == 2, do: assert(length(records) <= 2)
        {records, epoch + 1}

      retries > 0 ->
        fetch_until_records(conn, version, group_id, member_id, topic_id, epoch + 1, retries - 1)

      true ->
        flunk("ShareFetch v#{version} acquired no records")
    end
  end

  # Records in the response that are covered by acquired_records ranges.
  defp acquired(partition) do
    ranges = for a <- partition.acquired_records, do: a.first_offset..a.last_offset//1

    for a <- partition.acquired_records, do: assert(a.delivery_count == 1)

    for batch <- partition.records || [],
        record <- batch.records,
        Enum.any?(ranges, &(record.offset in &1)),
        do: {record.offset, record.value}
  end

  defp ack_batches(records) do
    for {offset, _value} <- records do
      %{first_offset: offset, last_offset: offset, acknowledge_types: [@accept]}
    end
  end

  defp acknowledge(conn, version, group_id, member_id, topic_id, epoch, records) do
    request = Kayrock.ShareAcknowledge.get_request_struct(version)

    request = %{
      request
      | group_id: group_id,
        member_id: member_id,
        share_session_epoch: epoch,
        topics: [
          %{
            topic_id: topic_id,
            partitions: [%{partition_index: 0, acknowledgement_batches: ack_batches(records)}]
          }
        ]
    }

    request = if version >= 2, do: %{request | is_renew_ack: false}, else: request

    response = call(conn, request)
    assert response.error_code == 0, inspect(response)

    if version >= 2, do: assert(response.acquisition_lock_timeout_ms > 0)

    assert [%{topic_id: ^topic_id, partitions: [%{partition_index: 0, error_code: 0}]}] =
             response.responses
  end

  # Keeps fetching (piggy-backing acknowledgements of the previous fetch on
  # the next ShareFetch) until all records were seen.
  defp drain(conn, group_id, member_id, topic_id, epoch, seen, pending \\ [], retries \\ 20)

  defp drain(conn, group_id, member_id, topic_id, epoch, seen, pending, retries) do
    if length(seen) >= @record_count and pending == [] do
      seen
    else
      assert retries > 0, "only saw #{inspect(seen)}"

      request =
        share_fetch_request(2, group_id, member_id, topic_id, epoch, ack_batches(pending))

      response = call(conn, request)
      assert response.error_code == 0, inspect(response)
      assert [%{partitions: [partition]}] = response.responses
      assert partition.error_code == 0
      assert partition.acknowledge_error_code == 0
      fetched = acquired(partition)

      drain(conn, group_id, member_id, topic_id, epoch + 1, seen ++ fetched, fetched, retries - 1)
    end
  end

  # Polls DescribeShareGroupOffsets `version` for partition 0 of `topic`
  # until `ready?.(partition)`.
  defp wait_for_offsets(conn, version, group_id, topic, topic_id, ready?, retries \\ 50) do
    request = Kayrock.DescribeShareGroupOffsets.get_request_struct(version)

    request = %{
      request
      | groups: [%{group_id: group_id, topics: [%{topic_name: topic, partitions: [0]}]}]
    }

    %{groups: [group]} = call(conn, request)
    assert group.error_code == 0, inspect(group)
    assert [%{topic_name: ^topic, topic_id: ^topic_id, partitions: [partition]}] = group.topics
    assert partition.partition_index == 0
    assert partition.error_code == 0

    cond do
      ready?.(partition) ->
        partition

      retries > 0 ->
        Process.sleep(200)
        wait_for_offsets(conn, version, group_id, topic, topic_id, ready?, retries - 1)

      true ->
        flunk("DescribeShareGroupOffsets v#{version}: #{inspect(partition)}")
    end
  end

  # The member leaving is applied asynchronously; retry while the group is
  # still reported non-empty (NON_EMPTY_GROUP = 68).
  defp with_group_empty(fun, retries \\ 50) do
    response = fun.()

    if response.error_code == 68 and retries > 0 do
      Process.sleep(200)
      with_group_empty(fun, retries - 1)
    else
      response
    end
  end

  # -- setup helpers -----------------------------------------------------------

  defp produce(client, topic, value) do
    record_set = %RecordBatch{records: [%Record{value: value}]}
    request = produce_messages_request(topic, [[record_set: record_set]], 1, 8)

    {:ok, %{responses: [%{partition_responses: [%{error_code: 0}]}]}} =
      Kayrock.client_call(client, request, :controller)
  end

  # The group coordinator (and __consumer_offsets) is created lazily; until it
  # is ready group requests get COORDINATOR_NOT_AVAILABLE / NOT_COORDINATOR.
  defp wait_for_coordinator(conn, group_id, retries \\ 100) do
    request = %Kayrock.FindCoordinator.V3.Request{key: group_id, key_type: 0}

    case call(conn, request) do
      %{error_code: 0} ->
        :ok

      _ when retries > 0 ->
        Process.sleep(200)
        wait_for_coordinator(conn, group_id, retries - 1)
    end
  end

  defp set_auto_offset_reset_earliest(conn, group_id) do
    request = %Kayrock.IncrementalAlterConfigs.V1.Request{
      resources: [
        %{
          resource_type: @group_resource_type,
          resource_name: group_id,
          configs: [%{name: "share.auto.offset.reset", config_operation: 0, value: "earliest"}]
        }
      ],
      validate_only: false
    }

    assert %{responses: [%{error_code: 0}]} = call(conn, request)
    :ok
  end

  # Sends `request` on the raw connection and deserializes the response,
  # requiring every byte to be consumed.
  defp call(conn, request) do
    request = %{request | correlation_id: rem(System.unique_integer([:positive]), 1_000_000_000)}
    request = %{request | client_id: "kayrock-share-groups"}

    :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
    {:ok, data} = Kayrock.BrokerConnection.recv(conn, 10_000)
    {response, rest} = Kayrock.Request.response_deserializer(request).(data)

    assert rest == <<>>, "#{inspect(request.__struct__)} left #{byte_size(rest)} bytes"
    assert response.correlation_id == request.correlation_id
    response
  end
end
