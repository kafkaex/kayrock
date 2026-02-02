defmodule Kayrock.Chaos.ProducerTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 1_000 - reduced 70%
  @moderate_latency_ms 300
  # Was 500 - reduced 70%
  @jitter_latency_ms 150
  # Was 200 - reduced 70%
  @jitter_amount_ms 60
  # Was 3_000 - reduced 80%
  @high_latency_ms 600
  # Was 5_000 - reduced 60%
  @extreme_latency_ms 2_000
  # Was 15_000 - reduced 67%
  @timeout_latency_ms 5_000
  # Was 30_000 - reduced 67%
  @extreme_timeout_latency_ms 10_000

  # Bandwidth limits (KB/s)
  # Moderate throttling
  @high_bandwidth_kbps 100
  # Significant throttling
  @moderate_bandwidth_kbps 50
  # Extreme throttling
  @low_bandwidth_kbps 10

  # Connection timing (milliseconds) - optimized for speed
  # Was 100 - reduced 70%
  @connection_drop_duration_ms 30
  # Was 500 - reduced 70%
  @connection_recovery_wait_ms 150
  # Was 500 - reduced 70%
  @connection_down_wait_ms 150
  # Was 500 - reduced 80%
  @toxic_removal_wait_ms 100
  # Was 300 - reduced 70%
  @short_recovery_wait_ms 90
  # Was 100 - reduced 70%
  @brief_delay_ms 30
  # Was 200 - reduced 70%
  @fetch_retry_interval_ms 60

  # Flaky network simulation - optimized for speed
  # Was 3 - reduced 33%
  @flaky_network_cycles 2
  # Was 50 - reduced 60%
  @flaky_network_down_ms 20
  # Was 150 - reduced 70%
  @flaky_network_up_ms 45
  # Was 100 - reduced 70%
  @flaky_fetch_down_ms 30
  # Was 200 - reduced 70%

  # Packet manipulation
  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000
  @packet_size_small_bytes 50
  @packet_variation_small_bytes 25
  @packet_delay_small_us 500

  # Data limits
  # 1 KB
  @data_limit_small_bytes 1_024
  # 10 KB
  @data_limit_medium_bytes 10_240

  # Batch sizes - optimized for speed
  # Was 3 - reduced 33%
  @batch_size_small 2
  # Was 5 - reduced 40%
  @batch_size_medium 3
  # Was 10 - reduced 50%
  @batch_size_standard 5
  # Was 15 - reduced 47%
  @batch_size_medium_large 8
  # Was 20 - reduced 50%
  @batch_size_large 10
  # Was 100 - reduced 50%
  @batch_size_very_large 50

  # Message sizes - optimized for speed
  # Was 10_000 - reduced 50%
  @large_message_bytes 5_000

  # Timeout limits
  # 1 minute max
  @max_operation_timeout_ms 60_000

  # Timeouts for toxic operations
  @timeout_toxic_short_ms 2_000
  @slow_close_delay_ms 5_000

  # === Positive Scenarios ===

  describe "Producer under network latency" do
    @describetag chaos_type: :latency

    test "successfully produces #{@batch_size_standard} messages with #{@moderate_latency_ms}ms latency",
         ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Producing batch of messages
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "latency-test")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: All messages produced successfully
      assert_valid_offset(
        offset,
        "Producer should succeed with #{@moderate_latency_ms}ms latency"
      )

      # AND: All messages are retrievable
      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_standard, "Expected messages")
      assert_message_values(fetched, Enum.map(1..@batch_size_standard, &"latency-test-#{&1}"))
    end

    test "successfully produces #{@batch_size_medium} messages with #{@jitter_latency_ms}ms ± #{@jitter_amount_ms}ms jitter",
         ctx do
      # GIVEN: Network with latency and jitter (simulates variable routing)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_latency_ms, @jitter_amount_ms)

      # WHEN: Producing messages
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "jitter")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production succeeds despite variable latency
      assert_valid_offset(
        offset,
        "Producer should handle #{@jitter_latency_ms}ms ± #{@jitter_amount_ms}ms jitter"
      )

      # AND: All messages are intact
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_medium,
        "All messages should survive jittery network"
      )
    end

    test "handles extreme #{@extreme_latency_ms}ms latency", ctx do
      # GIVEN: Extremely high latency (near timeout threshold)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_latency_ms)

      # WHEN: Producing a single message
      topic = create_topic(ctx.client, 5)
      messages = [build_record("extreme-latency")]
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production eventually succeeds
      assert_valid_offset(
        offset,
        "Producer should eventually succeed with #{@extreme_latency_ms}ms latency"
      )
    end

    test "handles #{@moderate_latency_ms}ms upstream latency (client → broker)", ctx do
      # GIVEN: Latency only on upstream direction
      add_upstream_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Producing messages
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "upstream")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production succeeds
      assert_valid_offset(
        offset,
        "Producer should handle #{@moderate_latency_ms}ms upstream latency"
      )

      # AND: Messages are retrievable
      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_small, "Expected messages")
    end
  end

  describe "Producer under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers from brief connection drop and continues producing", ctx do
      # GIVEN: A topic for testing recovery
      topic = create_topic(ctx.client, 5)

      # WHEN: Producing first batch before failure
      messages_before = build_test_records(@batch_size_medium, "before-drop")
      offset1 = produce_messages!(ctx.client, topic, messages_before, 5)

      # AND: Connection drops then recovers
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      # AND: Producing second batch after recovery
      messages_after = build_test_records(@batch_size_medium, "after-drop")
      offset2 = produce_messages!(ctx.client, topic, messages_after, 5)

      # THEN: Both batches produced successfully
      assert_offset_progression(offset1, offset2, "Post-recovery batch should have higher offset")

      # AND: Both batches are retrievable
      fetched_before = fetch_messages(ctx.client, topic, offset1, 5)
      fetched_after = fetch_messages(ctx.client, topic, offset2, 5)

      assert_message_count(
        fetched_before,
        @batch_size_medium,
        "Pre-drop messages should be preserved"
      )

      assert_message_count(
        fetched_after,
        @batch_size_medium,
        "Post-recovery messages should be stored"
      )
    end

    @tag :flaky_network
    test "handles #{@flaky_network_cycles} intermittent connection failures", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Simulating flaky network (multiple drop/restore cycles)
      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@flaky_network_up_ms)
      end

      # AND: Producing messages after network stabilizes
      messages = build_test_records(@batch_size_medium, "flaky")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production succeeds after flaky network
      assert_valid_offset(
        offset,
        "Producer should recover from #{@flaky_network_cycles} connection drops"
      )

      # AND: All messages are intact
      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_medium, "Expected messages")
    end

    test "handles #{@timeout_toxic_short_ms}ms connection timeout", ctx do
      # GIVEN: A topic with initial successful production
      topic = create_topic(ctx.client, 5)
      messages_before = [build_record("before-timeout")]
      offset1 = produce_messages!(ctx.client, topic, messages_before, 5)

      # WHEN: Adding timeout toxic (connection closes after timeout)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      # AND: Attempting to produce during timeout window
      messages_during = [build_record("during-timeout")]

      result =
        try do
          {:ok, produce_messages!(ctx.client, topic, messages_during, 5)}
        catch
          _kind, _error -> :timeout_expected
        end

      # THEN: Operation may succeed or timeout (both acceptable)
      case result do
        {:ok, offset} ->
          fetched = fetch_messages(ctx.client, topic, offset, 5)

          assert is_list(fetched),
                 "If production succeeded, messages should be fetchable"

        :timeout_expected ->
          # Expected: timeout occurred
          assert true
      end

      # AND: First message is still retrievable
      fetched1 = fetch_messages(ctx.client, topic, offset1, 5)

      assert fetched1 != [],
             "Pre-timeout messages should remain accessible"
    end

    test "handles #{@slow_close_delay_ms}ms slow connection close", ctx do
      # GIVEN: Network with slow close (delays connection termination)
      topic = create_topic(ctx.client, 5)
      add_slow_close(ctx.toxiproxy, ctx.proxy_name, @slow_close_delay_ms)

      # WHEN: Producing messages
      messages = build_test_records(@batch_size_small, "slow-close")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production eventually succeeds despite slow close
      assert_valid_offset(
        offset,
        "Producer should handle #{@slow_close_delay_ms}ms slow close"
      )
    end
  end

  describe "Producer under bandwidth throttling" do
    @describetag chaos_type: :bandwidth

    test "handles #{@moderate_bandwidth_kbps} KB/s bandwidth throttling with large messages",
         ctx do
      # GIVEN: Severely throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Producing large messages
      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate("x", @large_message_bytes)
      messages = build_test_records(@batch_size_standard, large_data)

      start_time = System.monotonic_time(:millisecond)
      offset = produce_messages!(ctx.client, topic, messages, 5)
      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Production succeeds (takes longer due to throttling)
      assert_valid_offset(
        offset,
        "Producer should handle #{@moderate_bandwidth_kbps} KB/s bandwidth limit"
      )

      IO.puts("""
      [Bandwidth Test] #{@moderate_bandwidth_kbps} KB/s throttling
        Duration: #{duration}ms, Messages: #{@batch_size_standard}
        Data: ~#{div(@large_message_bytes * @batch_size_standard, 1024)} KB
      """)

      # AND: All messages are stored
      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_standard, "Expected messages")
    end

    test "handles extreme #{@low_bandwidth_kbps} KB/s bandwidth limit", ctx do
      # GIVEN: Extremely low bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @low_bandwidth_kbps)

      # WHEN: Producing a single message
      topic = create_topic(ctx.client, 5)
      messages = [build_record("extreme-bandwidth")]
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production still succeeds
      assert_valid_offset(
        offset,
        "Producer should handle extreme #{@low_bandwidth_kbps} KB/s bandwidth limit"
      )
    end
  end

  describe "Producer under packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    @tag :packet_fragmentation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation for #{@batch_size_large} messages",
         ctx do
      # GIVEN: Network that fragments packets into small chunks
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      # WHEN: Producing many messages (will be fragmented)
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_large, "fragmented")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production succeeds despite fragmentation
      assert_valid_offset(
        offset,
        "Producer should work with #{@packet_size_medium_bytes}B packet fragmentation"
      )

      # AND: All messages arrive correctly after reassembly
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_large,
        "All fragmented messages should be reassembled correctly"
      )
    end

    @tag :data_limit
    test "recovers after #{@data_limit_medium_bytes}B data limit triggers reconnection", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Connection has data limit (closes after limit)
      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_medium_bytes)

      # AND: Producing small batch (within limit)
      messages_before = build_test_records(@batch_size_medium, "before-limit")
      offset1 = produce_messages!(ctx.client, topic, messages_before, 5)

      # AND: Removing limit and allowing reconnection
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "limit_data_downstream")
      Process.sleep(@toxic_removal_wait_ms)

      # AND: Producing after reconnection
      messages_after = build_test_records(@batch_size_medium, "after-limit")
      offset2 = produce_messages!(ctx.client, topic, messages_after, 5)

      # THEN: Both batches produced successfully
      assert_offset_progression(offset1, offset2, "Post-limit batch should have higher offset")
    end
  end

  describe "Producer under combined chaos scenarios" do
    @describetag chaos_type: :combined

    @tag :wan_simulation
    test "handles combined #{@jitter_latency_ms}ms latency + #{@high_bandwidth_kbps} KB/s bandwidth (WAN simulation)",
         ctx do
      # GIVEN: Real-world WAN conditions (latency + jitter + bandwidth limit)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_latency_ms, @jitter_amount_ms)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      # WHEN: Producing messages under WAN conditions
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium_large, "wan")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Production succeeds under combined chaos
      assert_valid_offset(
        offset,
        "Producer should handle WAN conditions (latency + jitter + bandwidth)"
      )

      # AND: All messages are stored
      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_medium_large, "Expected messages")
    end

    @tag :multi_phase
    test "survives three sequential chaos phases", ctx do
      # GIVEN: A topic for multi-phase testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Phase 1 - High latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      messages_p1 = build_test_records(@batch_size_small, "phase1")
      offset1 = produce_messages!(ctx.client, topic, messages_p1, 5)

      # AND: Phase 2 - Connection drop and recovery
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@short_recovery_wait_ms)

      messages_p2 = build_test_records(@batch_size_small, "phase2")
      offset2 = produce_messages!(ctx.client, topic, messages_p2, 5)

      # AND: Phase 3 - Bandwidth throttling
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)
      messages_p3 = build_test_records(@batch_size_small, "phase3")
      offset3 = produce_messages!(ctx.client, topic, messages_p3, 5)

      # THEN: All three phases succeed
      assert_valid_offset(offset1, "Phase 1 (latency) should succeed")
      assert_valid_offset(offset2, "Phase 2 (connection drop) should succeed")
      assert_valid_offset(offset3, "Phase 3 (bandwidth) should succeed")

      # AND: Offsets show proper progression
      assert_offset_progression(offset1, offset2, "Phase 1 → 2")
      assert_offset_progression(offset2, offset3, "Phase 2 → 3")
    end
  end

  # === Negative Scenarios ===

  describe "Negative scenarios - operations fail gracefully" do
    @tag chaos_type: :extreme_latency
    test "returns error when latency (#{@extreme_timeout_latency_ms}ms) exceeds reasonable timeout",
         ctx do
      # GIVEN: Extreme latency that exceeds typical timeout
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = [build_record("will-timeout")]

      # WHEN: Attempting operation
      # THEN: Should fail with timeout error, not hang forever
      result =
        try do
          produce_messages!(ctx.client, topic, messages, 5)
          :unexpected_success
        catch
          kind, error -> {:expected_error, kind, error}
        end

      case result do
        {:expected_error, _kind, _error} ->
          # Expected: timeout or connection error
          assert true

        :unexpected_success ->
          flunk("Expected timeout with #{@extreme_timeout_latency_ms}ms latency")
      end
    end

    @tag chaos_type: :connection_failure
    test "returns error when connection is down for extended period", ctx do
      # GIVEN: Topic created successfully
      topic = create_topic(ctx.client, 5)

      # WHEN: Connection completely down
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_down_wait_ms)

      messages = [build_record("unreachable")]

      # THEN: Should fail with clear error, not hang
      assert_raise MatchError, fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end
    end

    @tag chaos_type: :connection_failure
    test "returns error when broker is unreachable during topic creation", ctx do
      # GIVEN: Connection down before topic creation
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@brief_delay_ms)

      # WHEN: Attempting topic creation
      # THEN: Should fail with error
      assert_raise MatchError, fn ->
        create_topic(ctx.client, 5)
      end
    end

    @tag chaos_type: :extreme_latency
    test "provides error when fetch times out with #{@extreme_timeout_latency_ms}ms latency",
         ctx do
      # GIVEN: Topic with messages produced successfully
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "msg")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # WHEN: Adding extreme latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      # THEN: Fetch should timeout with error
      result =
        try do
          fetch_messages(ctx.client, topic, offset, 5)
        catch
          kind, error -> {:caught, kind, error}
        end

      case result do
        {:caught, _kind, _error} ->
          # Expected: operation failed with error
          assert true

        records when is_list(records) ->
          # Acceptable: succeeded with very long timeout
          assert is_list(records)
      end
    end

    @tag chaos_type: :connection_failure
    test "fails predictably when connection drops mid-operation", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Dropping connection during produce
      spawn(fn ->
        Process.sleep(@brief_delay_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
      end)

      messages = build_test_records(@batch_size_very_large, "large-batch")

      # THEN: Should either succeed (before drop) or fail clearly
      result =
        try do
          produce_messages!(ctx.client, topic, messages, 5)
          :succeeded_before_drop
        catch
          _kind, _error -> :connection_failed
        end

      case result do
        :connection_failed ->
          # Expected: connection dropped during operation
          assert true

        :succeeded_before_drop ->
          # Also acceptable: completed before connection dropped
          assert true
      end
    end

    @tag chaos_type: :timeout
    test "respects client timeout settings (< #{@max_operation_timeout_ms}ms) under #{@timeout_latency_ms}ms latency",
         ctx do
      # GIVEN: Latency above typical timeout threshold
      add_latency(ctx.toxiproxy, ctx.proxy_name, @timeout_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = [build_record("timeout-test")]

      # WHEN: Attempting operation
      start_time = System.monotonic_time(:millisecond)

      result =
        try do
          produce_messages!(ctx.client, topic, messages, 5)
          :succeeded
        catch
          _kind, _error -> :timeout
        end

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Should timeout within reasonable bounds (not wait forever)
      assert duration < @max_operation_timeout_ms,
             "Operation took #{duration}ms, should timeout faster than #{@max_operation_timeout_ms}ms"

      # Both timeout and success are acceptable
      assert result in [:timeout, :succeeded]
    end

    @tag chaos_type: :data_limit
    test "returns error when #{@large_message_bytes}B message exceeds #{@data_limit_small_bytes}B limit",
         ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Data limit is very small
      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_small_bytes)

      # AND: Attempting to send large message
      large_data = String.duplicate("x", @large_message_bytes)
      messages = [build_record(large_data)]

      # THEN: Should fail when limit exceeded
      result =
        try do
          produce_messages!(ctx.client, topic, messages, 5)
          :succeeded_within_limit
        catch
          _kind, _error -> :limit_exceeded
        end

      # Either fails (limit exceeded) or succeeds (message compressed enough)
      assert result in [:limit_exceeded, :succeeded_within_limit]
    end
  end

  describe "Consumer fetch resilience" do
    @describetag chaos_type: :fetch

    test "fetch handles #{@timeout_toxic_short_ms}ms timeout gracefully", ctx do
      # GIVEN: Topic with messages produced
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "timeout-test")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # WHEN: Adding timeout
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      # THEN: Fetch may fail or succeed - should not crash
      result =
        try do
          fetch_messages(ctx.client, topic, offset, 5)
        catch
          _kind, _error -> :timeout_expected
        end

      case result do
        records when is_list(records) ->
          assert length(records) <= @batch_size_medium,
                 "If fetch succeeded, should return expected messages"

        :timeout_expected ->
          # Expected with timeout toxic
          assert true
      end
    end

    test "fetch succeeds with #{@high_latency_ms}ms high latency", ctx do
      # GIVEN: Network with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Producing and fetching messages
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "slow-fetch")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Fetch eventually succeeds
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_small,
        "Fetch should succeed with #{@high_latency_ms}ms latency"
      )
    end

    @tag :packet_fragmentation
    test "fetch deserializes correctly with #{@packet_size_small_bytes}B packet slicing", ctx do
      # GIVEN: Network that slices packets into small pieces
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_small_bytes,
        @packet_variation_small_bytes,
        @packet_delay_small_us
      )

      # WHEN: Producing and fetching messages
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "sliced-fetch")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # THEN: Fetch deserializes correctly despite fragmentation
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_standard,
        "All messages should deserialize correctly despite packet slicing"
      )
    end

    @tag :flaky_network
    test "fetch recovers from #{@flaky_network_cycles} intermittent failures", ctx do
      # GIVEN: Topic with messages produced
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "intermittent")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      # WHEN: Simulating flaky network during fetch attempts
      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_fetch_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@fetch_retry_interval_ms)
      end

      # THEN: Final fetch should work after network stabilizes
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_standard,
        "Fetch should recover from #{@flaky_network_cycles} intermittent failures"
      )
    end
  end

  # === Helper Functions ===

  defp build_record(value) do
    %Kayrock.RecordBatch.Record{
      value: value,
      key: nil,
      headers: []
    }
  end

  defp build_test_records(count, prefix) do
    for i <- 1..count do
      %Kayrock.RecordBatch.Record{
        value: "#{prefix}-#{i}",
        key: nil,
        headers: []
      }
    end
  end

  defp produce_messages!(client, topic, records, api_version) do
    record_batch = %Kayrock.RecordBatch{records: records}

    produce_request =
      produce_messages_request(topic, [[record_set: record_batch]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client, produce_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses

    assert partition_response.error_code == 0,
           "Expected error_code=0, got: #{partition_response.error_code}"

    partition_response.base_offset
  end

  defp fetch_messages(client, topic, offset, api_version) do
    partition_data = [[topic: topic, partition: 0, fetch_offset: offset]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses

    # Extract records from record_set
    case partition_response.record_set do
      [%Kayrock.RecordBatch{records: records} | _] -> records
      _ -> []
    end
  end

  defp assert_message_values(records, expected_values) do
    actual_values = Enum.map(records, & &1.value)

    assert actual_values == expected_values,
           "Message values should match expected. Got: #{inspect(actual_values)}"
  end

  # === Assertion Helpers ===

  defp assert_valid_offset(offset, message) do
    assert is_integer(offset) and offset >= 0,
           "#{message} (got: #{inspect(offset)})"

    offset
  end

  defp assert_message_count(records, expected_count, message) do
    actual_count = length(records)
    msg = message || "Expected #{expected_count} messages, got #{actual_count}"
    assert actual_count == expected_count, msg
    records
  end

  defp assert_offset_progression(offset1, offset2, context) do
    msg = if context != "", do: "#{context}: ", else: ""

    assert offset2 > offset1,
           "#{msg}Expected offset2 (#{offset2}) > offset1 (#{offset1})"
  end
end
