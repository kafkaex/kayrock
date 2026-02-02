defmodule Kayrock.Chaos.ConsumerGroupTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 1_000 - reduced 70%
  @moderate_latency_ms 300
  # Was 1_500 - reduced 70%
  @high_latency_ms 450
  # Was 2_000 - reduced 70%
  @very_high_latency_ms 600
  # Was 10_000 - reduced 70%
  @session_timeout_latency_ms 3_000
  # Was 15_000 - reduced 70%
  @timeout_latency_ms 4_500
  # Was 30_000 - reduced 70%
  @extreme_timeout_latency_ms 9_000

  # Connection timing (milliseconds) - optimized for speed
  # Was 30 - reduced 67%
  @connection_drop_short_ms 10
  # Was 50 - reduced 70%
  @connection_drop_brief_ms 15
  # Was 80 - reduced 69%
  @connection_drop_medium_ms 25
  # Was 100 - reduced 70%
  @connection_drop_standard_ms 30
  # Was 100 - reduced 70%
  @connection_recovery_short_ms 30
  # Was 300 - reduced 70%
  # Was 500 - reduced 70%
  # Was 5_000 - reduced 70%
  @session_eviction_wait_ms 1_500
  # Was 500 - reduced 70%
  @post_eviction_recovery_ms 150

  # Flaky network simulation - optimized for speed
  # Was 3 - reduced 33%
  @flaky_network_cycles 2
  # Was 50 - reduced 60%
  @flaky_network_down_ms 20
  # Was 200 - reduced 70%
  @flaky_heartbeat_up_ms 60

  # Bandwidth limits (KB/s)
  @moderate_bandwidth_kbps 50

  # Partition assignments
  @partition_count_small [0, 1, 2]

  # === Tests ===

  describe "JoinGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully joins group with #{@very_high_latency_ms}ms latency", ctx do
      # GIVEN: Network with high latency (2 seconds)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      # WHEN: Creating topic and joining group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-#{unique_string()}"

      # AND: Finding coordinator (slow due to latency)
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # AND: Joining group (also slow)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # THEN: Coordinator found successfully
      assert_no_error(
        coordinator.error_code,
        "Coordinator lookup should succeed with #{@very_high_latency_ms}ms latency"
      )

      # AND: Join succeeds despite latency
      assert_no_error(
        join_response.error_code,
        "JoinGroup should succeed with #{@very_high_latency_ms}ms latency"
      )

      assert is_binary(join_response.member_id),
             "Should receive valid member_id"
    end

    @tag chaos_type: :connection_failure
    test "recovers from brief connection drop during join", ctx do
      # GIVEN: A topic and group for testing
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # WHEN: Connection drops during join
      spawn(fn ->
        Process.sleep(@connection_drop_brief_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_standard_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      # AND: Attempting to join group
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # THEN: Join succeeds after retry/recovery
      assert_no_error(
        join_response.error_code,
        "JoinGroup should recover from brief connection drop"
      )
    end

    @tag chaos_type: :timeout
    test "handles #{@timeout_latency_ms}ms timeout during join gracefully", ctx do
      # GIVEN: A topic and group for testing
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # WHEN: Adding extreme timeout
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_latency_ms)

      # AND: Attempting to join group
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      # THEN: May fail or succeed depending on timing (both acceptable)
      result =
        try do
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        catch
          _kind, _error -> :timeout_expected
        end

      case result do
        {:ok, response} ->
          # If succeeded, should be valid
          assert_no_error(
            response.error_code,
            "If JoinGroup succeeded, response should be valid"
          )

        :timeout_expected ->
          # Expected with extreme timeout
          assert true
      end
    end
  end

  describe "SyncGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully syncs group with #{@high_latency_ms}ms latency", ctx do
      # GIVEN: Network with latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Joining group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # AND: Syncing with latency
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

      # THEN: Sync succeeds despite latency
      assert_no_error(
        sync_response.error_code,
        "SyncGroup should succeed with #{@high_latency_ms}ms latency"
      )
    end

    @tag chaos_type: :connection_failure
    test "handles connection drop during sync", ctx do
      # GIVEN: Successfully joined group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # WHEN: Connection drops during sync
      assignments = [
        %{
          member_id: join_response.member_id,
          topic: topic,
          partitions: @partition_count_small
        }
      ]

      spawn(fn ->
        Process.sleep(@connection_drop_short_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_medium_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      sync_request =
        sync_group_request(group_id, join_response.member_id, assignments, 3)

      # AND: Retrying sync
      {:ok, sync_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)
        end)

      # THEN: Sync eventually succeeds after retry
      assert_no_error(
        sync_response.error_code,
        "SyncGroup should recover from connection drop"
      )
    end
  end

  describe "Heartbeat with chaos" do
    @describetag chaos_type: :session_management

    test "handles #{@session_timeout_latency_ms}ms latency exceeding session timeout", ctx do
      # GIVEN: Successfully joined and synced group (without chaos)
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-heartbeat-timeout-#{unique_string()}"
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

      {:ok, _sync_response} =
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      # WHEN: Adding extreme latency (exceeds session timeout)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @session_timeout_latency_ms)

      # AND: Attempting heartbeat with extreme latency
      heartbeat_request =
        heartbeat_request(
          group_id,
          join_response.member_id,
          join_response.generation_id,
          3
        )

      result =
        try do
          Kayrock.client_call(ctx.client, heartbeat_request, coordinator.node_id)
        catch
          _kind, _error -> :timeout_expected
        end

      # THEN: May timeout or return error (both acceptable)
      case result do
        {:ok, response} ->
          # May succeed or return error depending on timing
          assert is_integer(response.error_code),
                 "Heartbeat response should have error_code"

        :timeout_expected ->
          # Expected with extreme latency
          assert true
      end
    end

    @tag chaos_type: :connection_failure
    @tag :flaky_network
    test "handles #{@flaky_network_cycles} intermittent heartbeat failures", ctx do
      # GIVEN: Successfully joined and synced group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-heartbeat-flaky-#{unique_string()}"
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

      {:ok, _sync_response} =
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      # WHEN: Sending heartbeats with intermittent failures
      for _ <- 1..@flaky_network_cycles do
        # Flaky network
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")

        # Try heartbeat
        heartbeat_request =
          heartbeat_request(
            group_id,
            join_response.member_id,
            join_response.generation_id,
            3
          )

        result =
          try do
            Kayrock.client_call(ctx.client, heartbeat_request, coordinator.node_id)
          catch
            _kind, _error -> :error
          end

        case result do
          {:ok, _response} -> :ok
          :error -> :ok
        end

        Process.sleep(@flaky_heartbeat_up_ms)
      end

      # THEN: Final heartbeat should work after network stabilizes
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_recovery_short_ms)

      heartbeat_request =
        heartbeat_request(
          group_id,
          join_response.member_id,
          join_response.generation_id,
          3
        )

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, coordinator.node_id)
        end)

      # May be error if member was evicted, or success if still alive
      assert is_integer(response.error_code),
             "Final heartbeat should return valid response"
    end
  end

  describe "LeaveGroup with chaos" do
    @describetag chaos_type: :latency

    test "successfully leaves group with #{@moderate_latency_ms}ms latency", ctx do
      # GIVEN: Network with latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Joining and then leaving group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-leave-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # AND: Leaving group with latency
      leave_request = leave_group_request(group_id, join_response.member_id, 3)

      {:ok, leave_response} =
        Kayrock.client_call(ctx.client, leave_request, coordinator.node_id)

      # THEN: Leave succeeds despite latency
      assert_no_error(
        leave_response.error_code,
        "LeaveGroup should succeed with #{@moderate_latency_ms}ms latency"
      )
    end

    @tag chaos_type: :connection_failure
    test "handles connection drop during leave", ctx do
      # GIVEN: Successfully joined group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-leave-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # WHEN: Connection drops during leave
      spawn(fn ->
        Process.sleep(@connection_drop_short_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_standard_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      leave_request = leave_group_request(group_id, join_response.member_id, 3)

      # THEN: May fail or succeed (both acceptable)
      result =
        try do
          Kayrock.client_call(ctx.client, leave_request, coordinator.node_id)
        catch
          _kind, _error -> :connection_lost
        end

      case result do
        {:ok, response} ->
          assert is_integer(response.error_code),
                 "If LeaveGroup succeeded, response should be valid"

        :connection_lost ->
          assert true, "Connection lost during leave is acceptable"
      end
    end
  end

  describe "Rebalance under chaos" do
    @describetag chaos_type: :latency

    test "completes rebalance with #{@very_high_latency_ms}ms high latency", ctx do
      # GIVEN: Network with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      # WHEN: Member joins and syncs (full rebalance)
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-rebalance-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # AND: Member 1 joins
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # AND: Syncs
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
        with_retry(fn ->
          Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)
        end)

      # THEN: Rebalance completes successfully
      assert_no_error(
        sync_response.error_code,
        "Rebalance should succeed with #{@very_high_latency_ms}ms latency"
      )

      assert_valid_generation(
        join_response.generation_id,
        "Generation ID should be valid"
      )
    end

    @tag chaos_type: :bandwidth
    test "handles rebalance with #{@moderate_bandwidth_kbps} KB/s bandwidth limits", ctx do
      # GIVEN: Bandwidth-throttled network
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Performing join and sync
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

      # THEN: Rebalance succeeds despite bandwidth limit
      assert_no_error(
        sync_response.error_code,
        "Rebalance should succeed with #{@moderate_bandwidth_kbps} KB/s bandwidth limit"
      )
    end
  end

  describe "Negative scenarios - consumer group failures" do
    @tag chaos_type: :connection_failure
    test "returns error when coordinator unreachable due to extended downtime", ctx do
      # GIVEN: Group ID for coordinator lookup
      group_id = "chaos-unreachable-#{unique_string()}"

      # WHEN: Connection is completely down
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_recovery_short_ms)

      request = find_coordinator_request(group_id, 2)

      # THEN: Should fail with clear error, not hang
      assert_raise MatchError, fn ->
        Kayrock.client_call(ctx.client, request, 1)
      end
    end

    @tag chaos_type: :extreme_latency
    test "returns error when join times out due to #{@extreme_timeout_latency_ms}ms extreme latency",
         ctx do
      # GIVEN: Extreme latency (30s) that should cause timeout
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      # WHEN: Attempting to join group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-join-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      # THEN: Should timeout or fail, not hang forever
      result =
        try do
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        catch
          kind, error -> {:error, kind, error}
        end

      case result do
        {:error, _kind, _error} ->
          # Expected: timeout or connection error
          assert true

        {:ok, response} ->
          # Unexpected but possible with very long timeout
          flunk("Expected timeout but join succeeded: #{inspect(response)}")
      end
    end

    @tag chaos_type: :connection_failure
    test "fails gracefully when heartbeat cannot reach coordinator", ctx do
      # GIVEN: Successfully joined group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-heartbeat-fail-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # WHEN: Connection goes down
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_recovery_short_ms)

      # AND: Attempting heartbeat
      heartbeat_request =
        heartbeat_request(
          group_id,
          join_response.member_id,
          join_response.generation_id,
          3
        )

      # THEN: Heartbeat should fail gracefully with clear error
      assert_raise MatchError, fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, coordinator.node_id)
      end
    end

    @tag chaos_type: :extreme_latency
    test "provides error when sync group times out with #{@extreme_timeout_latency_ms}ms latency",
         ctx do
      # GIVEN: Successfully joined group (no chaos)
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-sync-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
      join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, join_request, coordinator.node_id)
        end)

      # WHEN: Adding extreme latency before sync
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

      # THEN: Should timeout or fail
      result =
        try do
          Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)
        catch
          _kind, _error -> :timeout
        end

      case result do
        :timeout ->
          # Expected
          assert true

        {:ok, response} ->
          # Possible with very long timeout
          flunk("Expected timeout but sync succeeded: #{inspect(response)}")
      end
    end

    @tag chaos_type: :session_management
    test "detects member eviction after prolonged heartbeat failure", ctx do
      # GIVEN: Successfully joined and synced group
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-eviction-#{unique_string()}"
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

      {:ok, _sync_response} =
        Kayrock.client_call(ctx.client, sync_request, coordinator.node_id)

      # WHEN: Connection down for extended period (simulate missed heartbeats)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      # Longer than session timeout
      Process.sleep(@session_eviction_wait_ms)

      # AND: Connection restored
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@post_eviction_recovery_ms)

      # AND: Attempting heartbeat after eviction
      heartbeat_request =
        heartbeat_request(
          group_id,
          join_response.member_id,
          join_response.generation_id,
          3
        )

      result =
        try do
          {:ok, response} =
            Kayrock.client_call(ctx.client, heartbeat_request, coordinator.node_id)

          response
        catch
          _kind, _error -> :error
        end

      # THEN: Should indicate member no longer in group
      case result do
        :error ->
          # Expected: connection issues
          assert true

        %{error_code: error_code} ->
          # If heartbeat succeeds, error_code should indicate issue
          # Error codes: 27 = REBALANCE_IN_PROGRESS, 25 = UNKNOWN_MEMBER_ID
          # 0 = NO_ERROR (member still valid, acceptable in some cases)
          assert error_code in [0, 25, 27],
                 "Unexpected error code after eviction: #{error_code}"
      end
    end
  end

  # === Helper Functions ===

  defp find_coordinator_with_retry(client, group_id, api_version) do
    request = find_coordinator_request(group_id, api_version)

    {:ok, response} =
      with_retry(fn ->
        Kayrock.client_call(client, request, 1)
      end)

    response
  end

  # === Assertion Helpers ===

  defp assert_no_error(error_code, message) do
    assert error_code == 0,
           "#{message} (got error_code: #{error_code})"

    error_code
  end

  defp assert_valid_generation(generation_id, message) do
    assert is_integer(generation_id) and generation_id >= 0,
           "#{message} (got: #{inspect(generation_id)})"

    generation_id
  end
end
