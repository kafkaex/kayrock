defmodule Kayrock.Chaos.ConsumerGroupTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @moderate_latency_ms 100
  @high_latency_ms 150
  @extreme_timeout_latency_ms 8_000

  @connection_drop_duration_ms 15
  @session_eviction_wait_ms 500
  @post_eviction_recovery_ms 50

  @flaky_network_cycles 2
  @flaky_network_down_ms 10
  @flaky_heartbeat_up_ms 30

  @moderate_bandwidth_kbps 50

  @timeout_toxic_ms 5

  @partition_count_small [0, 1, 2]

  describe "JoinGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully joins group with #{@moderate_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-#{unique_string()}"

      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      assert coordinator.error_code == 0

      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0
      assert is_binary(join_response.member_id)
    end

    @tag chaos_type: :connection_failure
    test "recovers from brief connection drop during join", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")

      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0
    end

    @tag chaos_type: :timeout
    test "fails when #{@timeout_toxic_ms}ms timeout closes connection during join", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
      end)
    end
  end

  describe "SyncGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully syncs group with #{@high_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      {:ok, sync_response} = Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      assert sync_response.error_code == 0
    end

    @tag chaos_type: :connection_failure
    test "handles connection drop during sync", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      {:ok, sync_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)
        end)

      assert sync_response.error_code == 0
    end
  end

  describe "Heartbeat with chaos" do
    @describetag chaos_type: :session_management

    @tag chaos_type: :connection_failure
    @tag :flaky_network
    test "survives #{@flaky_network_cycles} intermittent network failures without crashing",
         ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@flaky_heartbeat_up_ms)
      end

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert is_integer(response.error_code)
    end
  end

  describe "LeaveGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully leaves group with #{@moderate_latency_ms}ms latency", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-leave-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0
      assert is_binary(join_response.member_id) and join_response.member_id != ""

      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      leave_request = leave_group_request(group_id, join_response.member_id, 2)

      {:ok, leave_response} =
        Kayrock.client_call(ctx.client, leave_request, coordinator.node_id)

      assert leave_response.error_code == 0
    end

    @tag chaos_type: :connection_failure
    test "recovers from connection drop during leave", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-leave-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0
      assert is_binary(join_response.member_id) and join_response.member_id != ""

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")

      leave_request = leave_group_request(group_id, join_response.member_id, 2)

      {:ok, leave_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, leave_request, coordinator.node_id)
        end)

      assert is_integer(leave_response.error_code)
    end
  end

  describe "Rebalance under chaos" do
    @describetag chaos_type: :latency

    test "completes sync with #{@moderate_latency_ms}ms high latency", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-rebalance-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0

      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      {:ok, sync_response} =
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      assert sync_response.error_code == 0
      assert join_response.generation_id >= -1
    end

    @tag chaos_type: :bandwidth
    test "handles rebalance with #{@moderate_bandwidth_kbps} KB/s bandwidth limits", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      group_id = "chaos-rebalance-bw-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      {:ok, sync_response} =
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      assert sync_response.error_code == 0
    end
  end

  describe "Negative scenarios - consumer group failures" do
    @tag chaos_type: :connection_failure
    test "fails when coordinator unreachable due to extended downtime", ctx do
      group_id = "chaos-unreachable-#{unique_string()}"

      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      request = find_coordinator_request(group_id, 2)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, 1)
      end)
    end

    @tag chaos_type: :extreme_latency
    test "fails when join times out due to #{@extreme_timeout_latency_ms}ms extreme latency",
         ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
      end)
    end

    @tag chaos_type: :connection_failure
    test "fails when heartbeat cannot reach coordinator", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, node_id)
      end)
    end

    @tag chaos_type: :extreme_latency
    test "fails when sync group times out with #{@extreme_timeout_latency_ms}ms latency", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      assert join_response.error_code == 0
      assert is_binary(join_response.member_id)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)
      end)
    end

    @tag chaos_type: :session_management
    test "returns eviction error after prolonged heartbeat failure", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@session_eviction_wait_ms)

      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@post_eviction_recovery_ms)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert response.error_code in [25, 27],
             "Expected eviction error after session timeout, got error_code: #{response.error_code}"
    end
  end
end
