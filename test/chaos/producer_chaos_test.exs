defmodule Kayrock.Chaos.ProducerTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @moderate_latency_ms 150
  @jitter_latency_ms 80
  @jitter_amount_ms 30
  @high_latency_ms 300
  @extreme_latency_ms 800
  @extreme_timeout_latency_ms 6_000

  @high_bandwidth_kbps 100
  @moderate_bandwidth_kbps 50
  @low_bandwidth_kbps 10

  @connection_drop_duration_ms 10
  @connection_recovery_wait_ms 30
  @toxic_removal_wait_ms 30
  @short_recovery_wait_ms 30

  @flaky_network_cycles 2
  @flaky_network_down_ms 5
  @flaky_network_up_ms 15
  @flaky_fetch_down_ms 10
  @fetch_retry_interval_ms 20

  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000
  @packet_size_small_bytes 50
  @packet_variation_small_bytes 25
  @packet_delay_small_us 500

  @data_limit_small_bytes 1_024
  @data_limit_medium_bytes 10_240

  @batch_size_small 2
  @batch_size_medium 3
  @batch_size_standard 5
  @batch_size_medium_large 8
  @batch_size_large 10

  @large_message_bytes 5_000

  @timeout_toxic_short_ms 200
  @slow_close_delay_ms 5_000

  describe "Producer under network latency" do
    @describetag chaos_type: :latency

    test "successfully produces #{@batch_size_standard} messages with #{@moderate_latency_ms}ms latency",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "latency-test")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should succeed with #{@moderate_latency_ms}ms latency"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_standard, "Expected messages")
      assert_message_values(fetched, Enum.map(1..@batch_size_standard, &"latency-test-#{&1}"))
    end

    test "successfully produces #{@batch_size_medium} messages with #{@jitter_latency_ms}ms ± #{@jitter_amount_ms}ms jitter",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_latency_ms, @jitter_amount_ms)

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "jitter")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should handle #{@jitter_latency_ms}ms ± #{@jitter_amount_ms}ms jitter"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_medium,
        "All messages should survive jittery network"
      )
    end

    test "handles extreme #{@extreme_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = [build_record("extreme-latency")]
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should eventually succeed with #{@extreme_latency_ms}ms latency"
      )
    end

    test "handles #{@moderate_latency_ms}ms upstream latency (client → broker)", ctx do
      add_upstream_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "upstream")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should handle #{@moderate_latency_ms}ms upstream latency"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_small, "Expected messages")
    end
  end

  describe "Producer under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers from brief connection drop and continues producing", ctx do
      topic = create_topic(ctx.client, 5)

      messages_before = build_test_records(@batch_size_medium, "before-drop")
      offset1 = produce_messages!(ctx.client, topic, messages_before, 5)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      messages_after = build_test_records(@batch_size_medium, "after-drop")
      offset2 = produce_messages!(ctx.client, topic, messages_after, 5)

      assert_offset_progression(offset1, offset2, "Post-recovery batch should have higher offset")

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
      topic = create_topic(ctx.client, 5)

      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@flaky_network_up_ms)
      end

      messages = build_test_records(@batch_size_medium, "flaky")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should recover from #{@flaky_network_cycles} connection drops"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_medium, "Expected messages")
    end

    test "fails when #{@timeout_toxic_short_ms}ms timeout closes connection", ctx do
      topic = create_topic(ctx.client, 5)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      messages = [build_record("during-timeout")]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end

    test "handles #{@slow_close_delay_ms}ms slow connection close", ctx do
      topic = create_topic(ctx.client, 5)
      add_slow_close(ctx.toxiproxy, ctx.proxy_name, @slow_close_delay_ms)

      messages = build_test_records(@batch_size_small, "slow-close")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(offset, "Producer should handle #{@slow_close_delay_ms}ms slow close")
    end
  end

  describe "Producer under bandwidth throttling" do
    @describetag chaos_type: :bandwidth

    test "handles #{@moderate_bandwidth_kbps} KB/s bandwidth throttling with large messages",
         ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate("x", @large_message_bytes)
      messages = build_test_records(@batch_size_standard, large_data)
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should handle #{@moderate_bandwidth_kbps} KB/s bandwidth limit"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_standard, "Expected messages")
    end

    test "handles extreme #{@low_bandwidth_kbps} KB/s bandwidth limit", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @low_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      messages = [build_record("extreme-bandwidth")]
      offset = produce_messages!(ctx.client, topic, messages, 5)

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
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_large, "fragmented")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should work with #{@packet_size_medium_bytes}B packet fragmentation"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_large,
        "All fragmented messages should be reassembled correctly"
      )
    end

    @tag :data_limit
    test "recovers after #{@data_limit_medium_bytes}B data limit triggers reconnection", ctx do
      topic = create_topic(ctx.client, 5)

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_medium_bytes)

      messages_before = build_test_records(@batch_size_medium, "before-limit")
      offset1 = produce_messages!(ctx.client, topic, messages_before, 5)

      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "limit_data_downstream")
      Process.sleep(@toxic_removal_wait_ms)

      messages_after = build_test_records(@batch_size_medium, "after-limit")
      offset2 = produce_messages!(ctx.client, topic, messages_after, 5)

      assert_offset_progression(offset1, offset2, "Post-limit batch should have higher offset")
    end
  end

  describe "Producer under combined chaos scenarios" do
    @describetag chaos_type: :combined

    @tag :wan_simulation
    test "handles combined #{@jitter_latency_ms}ms latency + #{@high_bandwidth_kbps} KB/s bandwidth (WAN simulation)",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_latency_ms, @jitter_amount_ms)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium_large, "wan")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      assert_valid_offset(
        offset,
        "Producer should handle WAN conditions (latency + jitter + bandwidth)"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)
      assert_message_count(fetched, @batch_size_medium_large, "Expected messages")
    end

    @tag :multi_phase
    test "survives three sequential chaos phases", ctx do
      topic = create_topic(ctx.client, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      messages_p1 = build_test_records(@batch_size_small, "phase1")
      offset1 = produce_messages!(ctx.client, topic, messages_p1, 5)

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@short_recovery_wait_ms)

      messages_p2 = build_test_records(@batch_size_small, "phase2")
      offset2 = produce_messages!(ctx.client, topic, messages_p2, 5)

      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)
      messages_p3 = build_test_records(@batch_size_small, "phase3")
      offset3 = produce_messages!(ctx.client, topic, messages_p3, 5)

      assert_valid_offset(offset1, "Phase 1 (latency) should succeed")
      assert_valid_offset(offset2, "Phase 2 (connection drop) should succeed")
      assert_valid_offset(offset3, "Phase 3 (bandwidth) should succeed")

      assert_offset_progression(offset1, offset2, "Phase 1 → 2")
      assert_offset_progression(offset2, offset3, "Phase 2 → 3")
    end
  end

  describe "Negative scenarios - produce failures" do
    @describetag chaos_type: :negative

    test "fails when timeout toxic closes connection during produce", ctx do
      topic = create_topic(ctx.client, 5)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 100)

      messages = [build_record("will-timeout")]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end

    test "fails when connection is permanently down during produce", ctx do
      topic = create_topic(ctx.client, 5)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)
      Process.sleep(10)

      messages = [build_record("conn-down")]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end

    test "fails when #{@extreme_timeout_latency_ms}ms latency exceeds client timeout", ctx do
      topic = create_topic(ctx.client, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      messages = [build_record("timeout-test")]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end

    test "fails when #{@data_limit_small_bytes}B data limit is exceeded", ctx do
      topic = create_topic(ctx.client, 5)

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_small_bytes)

      large_data = String.duplicate("x", @large_message_bytes)
      messages = [build_record(large_data)]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end

    test "fails when connection closes immediately (0ms timeout)", ctx do
      topic = create_topic(ctx.client, 5)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      messages = [build_record("immediate-close")]

      assert_client_fails(fn ->
        produce_messages!(ctx.client, topic, messages, 5)
      end)
    end
  end

  describe "Consumer fetch resilience" do
    @describetag chaos_type: :fetch

    test "fails when #{@timeout_toxic_short_ms}ms timeout closes connection during fetch", ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "timeout-test")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      assert_client_fails(fn ->
        fetch_messages(ctx.client, topic, offset, 5)
      end)
    end

    test "fetch succeeds with #{@high_latency_ms}ms high latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "slow-fetch")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_small,
        "Fetch should succeed with #{@high_latency_ms}ms latency"
      )
    end

    @tag :packet_fragmentation
    test "fetch deserializes correctly with #{@packet_size_small_bytes}B packet slicing", ctx do
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_small_bytes,
        @packet_variation_small_bytes,
        @packet_delay_small_us
      )

      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "sliced-fetch")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_standard,
        "All messages should deserialize correctly despite packet slicing"
      )
    end

    @tag :flaky_network
    test "fetch recovers from #{@flaky_network_cycles} intermittent failures", ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_standard, "intermittent")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_fetch_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@fetch_retry_interval_ms)
      end

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_standard,
        "Fetch should recover from #{@flaky_network_cycles} intermittent failures"
      )
    end

    test "fails when #{@extreme_timeout_latency_ms}ms latency exceeds client timeout during fetch",
         ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "msg")
      offset = produce_messages!(ctx.client, topic, messages, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      assert_client_fails(fn ->
        fetch_messages(ctx.client, topic, offset, 5)
      end)
    end
  end

  defp build_record(value) do
    %Kayrock.RecordBatch.Record{
      value: value,
      key: nil,
      headers: []
    }
  end

  defp produce_messages!(client, topic, records, api_version) do
    produce_compressed!(client, topic, records, api_version, :none)
  end

  defp assert_message_values(records, expected_values) do
    actual_values = Enum.map(records, & &1.value)

    assert actual_values == expected_values,
           "Message values should match expected. Got: #{inspect(actual_values)}"
  end
end
