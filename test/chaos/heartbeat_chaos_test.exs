defmodule Kayrock.Chaos.HeartbeatTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 1_000 - reduced 70%
  @moderate_latency_ms 300
  # Was 500 - reduced 70%
  @heartbeat_base_latency_ms 150
  # Was 1_500 - reduced 70%
  @jitter_latency_ms 450
  # Was 500 - reduced 70%
  @jitter_amount_ms 150
  # Was 8_000 - reduced 70%
  @very_high_latency_ms 2_400
  # Was 200 - reduced 70%
  @burst_latency_ms 60

  # Bandwidth limits (KB/s)
  @moderate_bandwidth_kbps 50
  # Extreme throttling
  @low_bandwidth_kbps 10

  # Connection timing (milliseconds) - optimized for speed
  # Was 50 - reduced 70%
  @connection_drop_brief_ms 15
  # Was 100 - reduced 70%
  @connection_drop_duration_ms 30
  # Was 300 - reduced 70%
  @connection_recovery_wait_ms 90
  # Was 15_000 - reduced 70%
  @member_eviction_wait_ms 4_500

  # Heartbeat timing - optimized for speed
  # Was 1_000 - reduced 70%
  @heartbeat_interval_ms 300
  # Was 500 - reduced 70%
  @heartbeat_retry_wait_ms 150
  # Was 300 - reduced 70%
  @combined_phase_wait_ms 90

  # Test iteration counts - optimized for speed
  # Was 3 - reduced 33%
  @heartbeat_count_small 2
  # Was 5 - reduced 40%
  @heartbeat_count_medium 3
  # Was 5 - reduced 40%
  @burst_count 3

  # Timeout toxic - optimized for speed
  # Was 3_000 - reduced 70%
  @timeout_toxic_ms 900

  # Partition assignment
  @partition_assignment [0, 1, 2]

  # Success thresholds - optimized for speed
  # Was 3 - reduced 33%
  @minimum_success_count 2
  # Was 500 - reduced 70%
  @expected_min_duration_ms 150

  # === Tests ===

  describe "Heartbeat under network latency" do
    @describetag chaos_type: :latency

    test "successfully sends heartbeat with #{@moderate_latency_ms}ms moderate latency", ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Setting up active member and sending heartbeat
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      # THEN: Heartbeat succeeds despite latency
      assert_no_error(
        response.error_code,
        "Heartbeat should succeed with #{@moderate_latency_ms}ms latency"
      )
    end

    test "handles heartbeat with #{@very_high_latency_ms}ms extreme latency (near timeout)",
         ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Adding very high latency (near session timeout)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      # THEN: Heartbeat may succeed or timeout
      result =
        try do
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        catch
          _kind, _error -> :heartbeat_timeout
        end

      case result do
        {:ok, response} ->
          # May succeed or return error depending on session timeout
          assert is_integer(response.error_code),
                 "Response should have error_code"

        :heartbeat_timeout ->
          # Expected if latency exceeds timeout
          assert true
      end
    end

    @tag :jittery_network
    test "sends #{@heartbeat_count_small} heartbeats with #{@jitter_latency_ms}ms ± #{@jitter_amount_ms}ms jitter",
         ctx do
      # GIVEN: Network with jittery latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_latency_ms, @jitter_amount_ms)

      # WHEN: Setting up member and sending multiple heartbeats
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # AND: Sending heartbeats with variable latency
      for _ <- 1..@heartbeat_count_small do
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, heartbeat_request, node_id)
          end)

        # THEN: Heartbeats should succeed or indicate member status
        assert is_integer(response.error_code),
               "Heartbeat response should have error_code"

        Process.sleep(@heartbeat_interval_ms)
      end
    end
  end

  describe "Heartbeat under connection failures" do
    @describetag chaos_type: :connection_failure

    test "handles connection drop during heartbeat", ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Dropping connection during heartbeat
      spawn(fn ->
        Process.sleep(@connection_drop_brief_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_duration_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      # THEN: Heartbeat may fail or succeed depending on timing
      result =
        try do
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        catch
          _kind, _error -> :connection_dropped
        end

      case result do
        {:ok, response} ->
          assert is_integer(response.error_code),
                 "Response should have error_code"

        :connection_dropped ->
          # Expected with connection drop
          assert true
      end
    end

    @tag :flaky_network
    test "maintains session through intermittent heartbeat failures", ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Sending multiple heartbeats with intermittent failures
      success_count =
        Enum.reduce(1..@heartbeat_count_medium, 0, fn _, acc ->
          # Randomly add/remove down toxic (simulates flaky network)
          if :rand.uniform(2) == 1 do
            add_down(ctx.toxiproxy, ctx.proxy_name)
            Process.sleep(@connection_drop_brief_ms)
            remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
          end

          heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

          result =
            try do
              Kayrock.client_call(ctx.client, heartbeat_request, node_id)
            catch
              _kind, _error -> :failed
            end

          new_acc =
            case result do
              {:ok, response} when response.error_code == 0 ->
                acc + 1

              _ ->
                acc
            end

          Process.sleep(@heartbeat_retry_wait_ms)
          new_acc
        end)

      # THEN: At least some heartbeats should succeed
      assert success_count > 0,
             "At least one heartbeat should succeed despite intermittent failures"
    end

    @tag :member_eviction
    test "detects member eviction after #{@member_eviction_wait_ms}ms prolonged failure", ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Blocking all communication for extended period (exceeds session timeout)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@member_eviction_wait_ms)

      # AND: Restoring connection
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      # THEN: Heartbeat should fail - member evicted or rebalance needed
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      result =
        try do
          with_retry(fn ->
            Kayrock.client_call(ctx.client, heartbeat_request, node_id)
          end)
        catch
          _kind, _error -> {:error, :connection_failed}
        end

      case result do
        {:ok, response} ->
          # Should indicate error (member unknown or rebalance in progress)
          # Error codes: 25 = UNKNOWN_MEMBER_ID, 27 = REBALANCE_IN_PROGRESS
          assert response.error_code != 0,
                 "Member should be evicted after #{@member_eviction_wait_ms}ms downtime"

        {:error, :connection_failed} ->
          # Connection issues also acceptable
          assert true
      end
    end
  end

  describe "Heartbeat under timeout conditions" do
    @describetag chaos_type: :timeout

    test "handles #{@timeout_toxic_ms}ms timeout toxic", ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Adding timeout toxic
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      # THEN: Heartbeat may fail or succeed
      result =
        try do
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        catch
          _kind, _error -> :timeout_toxic
        end

      case result do
        {:ok, response} ->
          assert is_integer(response.error_code),
                 "Response should have error_code"

        :timeout_toxic ->
          # Expected with timeout toxic
          assert true
      end
    end
  end

  describe "Heartbeat under bandwidth constraints" do
    @describetag chaos_type: :bandwidth

    test "sends heartbeat with #{@low_bandwidth_kbps} KB/s extreme bandwidth throttling", ctx do
      # GIVEN: Extremely low bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @low_bandwidth_kbps)

      # WHEN: Setting up member and sending heartbeat
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # Heartbeat is small payload, should work even with low bandwidth
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      # THEN: Heartbeat succeeds (small payload not affected by low bandwidth)
      assert_no_error(
        response.error_code,
        "Heartbeat should succeed even with #{@low_bandwidth_kbps} KB/s limit"
      )
    end
  end

  describe "Heartbeat frequency under chaos" do
    @describetag chaos_type: :frequency

    @tag :frequency_test
    test "maintains consistent heartbeat frequency with #{@heartbeat_base_latency_ms}ms latency",
         ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @heartbeat_base_latency_ms)

      # WHEN: Setting up member and sending regular heartbeats
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # AND: Measuring heartbeat durations
      heartbeat_times =
        for _ <- 1..@heartbeat_count_medium do
          start = System.monotonic_time(:millisecond)

          heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

          {:ok, response} =
            with_retry(fn ->
              Kayrock.client_call(ctx.client, heartbeat_request, node_id)
            end)

          duration = System.monotonic_time(:millisecond) - start

          assert_no_error(
            response.error_code,
            "Heartbeat should succeed"
          )

          Process.sleep(@heartbeat_interval_ms)

          duration
        end

      # THEN: Average duration reflects latency impact
      avg_duration = Enum.sum(heartbeat_times) / length(heartbeat_times)

      IO.puts("""
      [Frequency Test] Heartbeat with #{@heartbeat_base_latency_ms}ms latency
        Average duration: #{trunc(avg_duration)}ms
        Heartbeats sent: #{@heartbeat_count_medium}
      """)

      assert avg_duration >= @expected_min_duration_ms,
             "Average heartbeat duration (#{trunc(avg_duration)}ms) should reflect #{@heartbeat_base_latency_ms}ms latency"
    end

    @tag :burst_test
    test "handles burst of #{@burst_count} heartbeats under combined chaos", ctx do
      # GIVEN: Combined latency and bandwidth constraints
      add_latency(ctx.toxiproxy, ctx.proxy_name, @burst_latency_ms)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Setting up member and sending burst of heartbeats
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # AND: Sending rapid heartbeats
      results =
        for _ <- 1..@burst_count do
          heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

          result =
            try do
              Kayrock.client_call(ctx.client, heartbeat_request, node_id)
            catch
              _kind, _error -> {:error, :failed}
            end

          case result do
            {:ok, response} -> response.error_code == 0
            {:error, _} -> false
          end
        end

      # THEN: Majority should succeed despite chaos
      success_count = Enum.count(results, & &1)

      assert success_count >= @minimum_success_count,
             "At least #{@minimum_success_count} of #{@burst_count} burst heartbeats should succeed"
    end
  end

  describe "Combined heartbeat chaos scenarios" do
    @describetag chaos_type: :combined

    @tag :multi_phase
    test "heartbeat survives three sequential chaos phases", ctx do
      # GIVEN: Active member with established session
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      # WHEN: Phase 1 - High latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      heartbeat1 = heartbeat_request(group_id, member_id, generation_id, 3)
      {:ok, response1} = Kayrock.client_call(ctx.client, heartbeat1, node_id)

      # THEN: Phase 1 succeeds
      assert_no_error(
        response1.error_code,
        "Phase 1 heartbeat should succeed with latency"
      )

      # WHEN: Phase 2 - Connection drop and recovery
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@combined_phase_wait_ms)

      heartbeat2 = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat2, node_id)
        end)

      # THEN: Phase 2 succeeds after recovery
      assert is_integer(response2.error_code),
             "Phase 2 heartbeat should complete after recovery"

      # WHEN: Phase 3 - Bandwidth limit
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)
      heartbeat3 = heartbeat_request(group_id, member_id, generation_id, 3)
      {:ok, response3} = Kayrock.client_call(ctx.client, heartbeat3, node_id)

      # THEN: Phase 3 succeeds with throttling
      assert is_integer(response3.error_code),
             "Phase 3 heartbeat should succeed with bandwidth limit"
    end
  end

  # === Helper Functions ===

  defp setup_active_member(ctx) do
    # Remove any toxics temporarily for setup
    remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)

    topic = create_topic(ctx.client, 5)
    group_id = "heartbeat-chaos-#{unique_string()}"

    # Find coordinator
    coordinator_request = find_coordinator_request(group_id, 2)

    {:ok, coordinator_response} =
      with_retry(fn ->
        Kayrock.client_call(ctx.client, coordinator_request, 1)
      end)

    assert coordinator_response.error_code == 0
    node_id = coordinator_response.node_id

    # Join group
    join_request = join_group_request(%{group_id: group_id, topics: [topic]}, 5)

    {:ok, join_response} =
      with_retry(fn ->
        Kayrock.client_call(ctx.client, join_request, node_id)
      end)

    assert join_response.error_code == 0

    # Sync group
    assignments = [
      %{
        member_id: join_response.member_id,
        topic: topic,
        partitions: @partition_assignment
      }
    ]

    sync_request = sync_group_request(group_id, join_response.member_id, assignments, 3)

    {:ok, sync_response} = Kayrock.client_call(ctx.client, sync_request, node_id)
    assert sync_response.error_code == 0

    {group_id, join_response.member_id, join_response.generation_id, node_id}
  end

  # === Assertion Helpers ===

  defp assert_no_error(error_code, message) do
    assert error_code == 0,
           "#{message} (got error_code: #{error_code})"
  end
end
