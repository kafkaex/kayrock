defmodule Kayrock.Chaos.CompressionTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @compression_types [:gzip, :snappy, :lz4]

  @high_latency_ms 400
  @moderate_latency_ms 200
  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  @packet_size_small_bytes 50
  @packet_variation_bytes 25
  @packet_delay_us 100
  @packet_size_medium_bytes 100
  @packet_delay_ms 1

  @data_limit_small_bytes 5_120
  @connection_drop_duration_ms 10
  @connection_recovery_wait_ms 30
  @post_toxic_removal_wait_ms 30
  @flaky_network_down_ms 5
  @flaky_network_up_ms 20
  @flaky_network_cycles 2

  @compressible_unit "compress! "
  @compressible_reps 500
  @large_data_size_bytes 5_000
  @fragment_data_unit "frag "
  @fragment_data_reps 200
  @highly_compressible_unit "aaaa"

  @batch_size_small 2
  @batch_size_medium 3
  @batch_size_large 5

  @timeout_toxic_ms 10
  @data_limit_too_small_bytes 50
  @extreme_timeout_latency_ms 6_000

  describe "Compressed production with network latency" do
    @describetag chaos_type: :latency

    for compression <- @compression_types do
      @tag compression_type: compression
      test "#{compression} compression succeeds with #{@high_latency_ms}ms latency", ctx do
        compression_type = unquote(compression)
        add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

        topic = create_topic(ctx.client, 5)
        large_data = String.duplicate(@compressible_unit, @compressible_reps)
        messages = build_test_records(@batch_size_medium, large_data)

        offset = produce_compressed!(ctx.client, topic, messages, 5, compression_type)

        assert_valid_offset(
          offset,
          "#{compression_type} compression should work with #{@high_latency_ms}ms latency"
        )

        fetched = fetch_messages(ctx.client, topic, offset, 5)

        assert_message_count(
          fetched,
          @batch_size_medium,
          "All #{compression_type} compressed messages should be retrievable"
        )
      end
    end

    @tag compression_type: :gzip
    @tag :bandwidth_efficiency
    test "gzip compression benefits from bandwidth throttling with large messages", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate("x", @large_data_size_bytes)
      messages = build_test_records(@batch_size_large, large_data)

      offset = produce_compressed!(ctx.client, topic, messages, 5, :gzip)

      assert_valid_offset(
        offset,
        "gzip should handle #{@moderate_bandwidth_kbps} KB/s bandwidth limit"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_large,
        "All compressed messages should be stored despite bandwidth limit"
      )
    end
  end

  describe "Compressed production with connection failures" do
    @describetag chaos_type: :connection_failure

    @tag compression_type: :gzip
    test "gzip recovers from brief connection drop", ctx do
      topic = create_topic(ctx.client, 5)

      messages_before = build_test_records(@batch_size_small, "before-drop")
      offset1 = produce_compressed!(ctx.client, topic, messages_before, 5, :gzip)

      disable_proxy(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      enable_proxy(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_recovery_wait_ms)

      messages_after = build_test_records(@batch_size_small, "after-drop")
      offset2 = produce_compressed!(ctx.client, topic, messages_after, 5, :gzip)

      assert_offset_progression(offset1, offset2, "Post-recovery batch should have higher offset")

      fetched_before = fetch_messages(ctx.client, topic, offset1, 5)
      fetched_after = fetch_messages(ctx.client, topic, offset2, 5)

      assert_message_count(
        fetched_before,
        @batch_size_small,
        "Pre-drop gzip messages should be preserved"
      )

      assert_message_count(
        fetched_after,
        @batch_size_small,
        "Post-recovery gzip messages should be stored"
      )
    end

    @tag compression_type: :snappy
    @tag :flaky_network
    test "snappy compression handles intermittent connection failures", ctx do
      topic = create_topic(ctx.client, 5)

      for _ <- 1..@flaky_network_cycles do
        disable_proxy(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        enable_proxy(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_up_ms)
      end

      messages = build_test_records(@batch_size_medium, "snappy")
      offset = produce_compressed!(ctx.client, topic, messages, 5, :snappy)

      assert_valid_offset(
        offset,
        "snappy should recover from #{@flaky_network_cycles} connection drops"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_medium,
        "All snappy messages should survive flaky network"
      )
    end
  end

  describe "Compressed production with packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    @tag compression_type: :gzip
    @tag :packet_fragmentation
    test "gzip decompresses correctly with #{@packet_size_small_bytes}B packet fragmentation",
         ctx do
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_small_bytes,
        @packet_variation_bytes,
        @packet_delay_us
      )

      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate(@fragment_data_unit, @fragment_data_reps)
      messages = build_test_records(@batch_size_medium, large_data)

      offset = produce_compressed!(ctx.client, topic, messages, 5, :gzip)

      assert_valid_offset(
        offset,
        "gzip should work with #{@packet_size_small_bytes}B packet fragmentation"
      )

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_medium,
        "All fragmented compressed messages should be reassembled"
      )

      for record <- fetched do
        assert String.starts_with?(record.value, large_data),
               "Decompressed content should match original despite fragmentation"
      end
    end

    @tag compression_type: :lz4
    @tag :data_limit
    test "lz4 compression works within #{@data_limit_small_bytes}B data limit", ctx do
      topic = create_topic(ctx.client, 5)

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_small_bytes)

      messages = build_test_records(@batch_size_small, "lz4")
      offset = produce_compressed!(ctx.client, topic, messages, 5, :lz4)

      assert_valid_offset(
        offset,
        "lz4 should succeed within #{@data_limit_small_bytes}B data limit"
      )

      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "limit_data_downstream")
      Process.sleep(@post_toxic_removal_wait_ms)

      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_small,
        "lz4 messages should be stored despite connection close"
      )
    end
  end

  describe "Combined compression and chaos scenarios" do
    @describetag chaos_type: :combined

    @tag compression_type: :gzip
    @tag :multi_phase
    test "gzip compression survives three sequential chaos phases", ctx do
      topic = create_topic(ctx.client, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      messages_p1 = build_test_records(@batch_size_small, "phase1")
      offset1 = produce_compressed!(ctx.client, topic, messages_p1, 5, :gzip)

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)
      messages_p2 = build_test_records(@batch_size_small, "phase2")
      offset2 = produce_compressed!(ctx.client, topic, messages_p2, 5, :gzip)

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_slicer(ctx.toxiproxy, ctx.proxy_name, @packet_size_medium_bytes, 50, @packet_delay_ms)
      messages_p3 = build_test_records(@batch_size_small, "phase3")
      offset3 = produce_compressed!(ctx.client, topic, messages_p3, 5, :gzip)

      assert_valid_offset(offset1, "Phase 1 (latency) should succeed")
      assert_valid_offset(offset2, "Phase 2 (bandwidth) should succeed")
      assert_valid_offset(offset3, "Phase 3 (fragmentation) should succeed")

      assert_offset_progression(offset1, offset2, "Phase 1 → 2")
      assert_offset_progression(offset2, offset3, "Phase 2 → 3")

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)

      fetched1 = fetch_messages(ctx.client, topic, offset1, 5)
      fetched2 = fetch_messages(ctx.client, topic, offset2, 5)
      fetched3 = fetch_messages(ctx.client, topic, offset3, 5)

      assert_message_count(fetched1, @batch_size_small, "Phase 1 messages intact")
      assert_message_count(fetched2, @batch_size_small, "Phase 2 messages intact")
      assert_message_count(fetched3, @batch_size_small, "Phase 3 messages intact")
    end

    @tag :performance_comparison
    @tag :bandwidth_efficiency
    test "compares gzip vs snappy efficiency under bandwidth constraints", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)
      compressible = String.duplicate(@highly_compressible_unit, @compressible_reps)

      results =
        for compression <- [:gzip, :snappy] do
          messages = build_test_records(@batch_size_medium, compressible)
          offset = produce_compressed!(ctx.client, topic, messages, 5, compression)

          assert_valid_offset(
            offset,
            "#{compression} should succeed with #{@moderate_bandwidth_kbps} KB/s limit"
          )

          {compression, offset}
        end

      assert length(results) == 2, "Both gzip and snappy should complete"
    end
  end

  describe "Negative scenarios - compression failures" do
    @describetag chaos_type: :negative

    @tag compression_type: :gzip
    @tag chaos_type: :timeout
    test "returns error when #{@timeout_toxic_ms}ms timeout closes connection during gzip produce",
         ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_medium, "will-timeout")

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      result = try_produce_compressed(ctx.client, topic, messages, 5, :gzip)

      assert_produce_failed(
        result,
        "gzip produce should fail with #{@timeout_toxic_ms}ms timeout toxic"
      )
    end

    @tag compression_type: :snappy
    @tag :connection_down
    test "returns error when connection is down during snappy produce", ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "conn-down")

      disable_proxy(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(10)

      result = try_produce_compressed(ctx.client, topic, messages, 5, :snappy)
      assert_produce_failed(result, "snappy produce should fail when connection is down")
    end

    @tag compression_type: :lz4
    @tag :data_limit
    test "returns error when data limit is too small for lz4 request", ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "exceeds-limit")

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_too_small_bytes)

      result = try_produce_compressed(ctx.client, topic, messages, 5, :lz4)

      assert_produce_failed(
        result,
        "lz4 produce should fail with #{@data_limit_too_small_bytes}B data limit"
      )
    end

    @tag compression_type: :gzip
    @tag :extreme_latency
    test "returns error or timeout with #{@extreme_timeout_latency_ms}ms extreme latency", ctx do
      topic = create_topic(ctx.client, 5)
      messages = build_test_records(@batch_size_small, "extreme-latency")

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      result = try_produce_compressed(ctx.client, topic, messages, 5, :gzip)

      assert_produce_failed(
        result,
        "gzip produce should fail with #{@extreme_timeout_latency_ms}ms latency"
      )
    end

    for compression <- @compression_types do
      @tag compression_type: compression
      @tag :immediate_close
      test "#{compression} returns error when connection closes immediately (0ms timeout)", ctx do
        compression_type = unquote(compression)

        topic = create_topic(ctx.client, 5)
        messages = build_test_records(@batch_size_small, "immediate-close")

        add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

        result = try_produce_compressed(ctx.client, topic, messages, 5, compression_type)

        assert_produce_failed(
          result,
          "#{compression_type} produce should fail when connection closes immediately"
        )
      end
    end
  end
end
