defmodule Kayrock.Chaos.CompressionTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Compression types to test
  @compression_types [:gzip, :snappy, :lz4, :zstd]

  # Network chaos parameters (OPTIMIZED FOR SPEED)
  # High latency - fast test
  @high_latency_ms 800
  # Moderate latency
  @moderate_latency_ms 400
  # Throttled network
  @moderate_bandwidth_kbps 50
  # Moderate throttling
  @high_bandwidth_kbps 100

  # Packet manipulation
  @packet_size_small_bytes 50
  @packet_variation_bytes 25
  # Reduced from 500
  @packet_delay_us 100
  @packet_size_medium_bytes 100
  @packet_delay_ms 1

  # Data limits
  # 5 KB
  @data_limit_small_bytes 5_120

  # Connection timing (OPTIMIZED)
  # Just enough to drop
  @connection_drop_duration_ms 30
  # Faster recovery
  @connection_recovery_wait_ms 150
  # Reduced wait
  @post_toxic_removal_wait_ms 100
  # Minimal down time
  @flaky_network_down_ms 20
  # Faster recovery
  @flaky_network_up_ms 80
  # Fewer cycles
  @flaky_network_cycles 2

  # Test data configuration (OPTIMIZED)
  @compressible_unit "compress! "
  # Half size - faster
  @compressible_reps 500
  # Half size
  @large_data_size_bytes 5_000
  @fragment_data_unit "frag "
  # Much smaller
  @fragment_data_reps 200
  @highly_compressible_unit "aaaa"

  # Batch sizes (OPTIMIZED)
  @batch_size_small 2
  @batch_size_medium 3
  @batch_size_large 5

  # === Tests ===

  describe "Compressed production with network latency" do
    @describetag chaos_type: :latency

    for compression <- @compression_types do
      @tag compression_type: compression
      test "#{compression} compression succeeds with #{@high_latency_ms}ms latency", ctx do
        compression_type = unquote(compression)

        # GIVEN: Network with high latency (satellite-like)
        add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

        # WHEN: Producing compressed messages with compressible data
        topic = create_topic(ctx.client, 5)
        large_data = String.duplicate(@compressible_unit, @compressible_reps)
        messages = build_test_records(@batch_size_medium, large_data)

        offset = produce_compressed!(ctx.client, topic, messages, 5, compression_type)

        # THEN: Production succeeds despite latency
        assert_valid_offset(
          offset,
          "#{compression_type} compression should work with #{@high_latency_ms}ms latency"
        )

        # AND: All messages are retrievable and decompress correctly
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
      # GIVEN: Throttled bandwidth (50 KB/s)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Producing large compressible messages
      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate("x", @large_data_size_bytes)
      messages = build_test_records(@batch_size_large, large_data)

      start_time = System.monotonic_time(:millisecond)
      offset = produce_compressed!(ctx.client, topic, messages, 5, :gzip)
      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Production succeeds (compression helps with bandwidth limit)
      assert_valid_offset(
        offset,
        "gzip should handle #{@moderate_bandwidth_kbps} KB/s bandwidth limit"
      )

      # AND: All messages are stored and retrievable
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_large,
        "All compressed messages should be stored despite bandwidth limit"
      )

      IO.puts("""
      [Bandwidth Test] gzip with #{@moderate_bandwidth_kbps} KB/s limit
        Duration: #{duration}ms, Messages: #{@batch_size_large}
        Data: ~#{div(@large_data_size_bytes * @batch_size_large, 1024)} KB uncompressed
      """)
    end
  end

  describe "Compressed production with connection failures" do
    @describetag chaos_type: :connection_failure

    @tag compression_type: :gzip
    test "gzip recovers from brief connection drop", ctx do
      # GIVEN: A topic for testing recovery
      topic = create_topic(ctx.client, 5)

      # WHEN: Producing batch before connection drop
      messages_before = build_test_records(@batch_size_small, "before-drop")
      offset1 = produce_compressed!(ctx.client, topic, messages_before, 5, :gzip)

      # AND: Connection drops then recovers
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      # AND: Producing batch after recovery
      messages_after = build_test_records(@batch_size_small, "after-drop")
      offset2 = produce_compressed!(ctx.client, topic, messages_after, 5, :gzip)

      # THEN: Both batches produced successfully
      assert_offset_progression(offset1, offset2, "Post-recovery batch should have higher offset")

      # AND: Both batches are retrievable
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
      messages = build_test_records(@batch_size_medium, "snappy")
      offset = produce_compressed!(ctx.client, topic, messages, 5, :snappy)

      # THEN: Production succeeds after flaky network
      assert_valid_offset(
        offset,
        "snappy should recover from #{@flaky_network_cycles} connection drops"
      )

      # AND: All messages are intact
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
      # GIVEN: Network that fragments packets into small chunks
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_small_bytes,
        @packet_variation_bytes,
        @packet_delay_us
      )

      # WHEN: Producing compressed messages (will be fragmented)
      topic = create_topic(ctx.client, 5)
      large_data = String.duplicate(@fragment_data_unit, @fragment_data_reps)
      messages = build_test_records(@batch_size_medium, large_data)

      offset = produce_compressed!(ctx.client, topic, messages, 5, :gzip)

      # THEN: Production succeeds despite aggressive fragmentation
      assert_valid_offset(
        offset,
        "gzip should work with #{@packet_size_small_bytes}B packet fragmentation"
      )

      # AND: Messages decompress correctly after reassembly
      fetched = fetch_messages(ctx.client, topic, offset, 5)

      assert_message_count(
        fetched,
        @batch_size_medium,
        "All fragmented compressed messages should be reassembled"
      )

      # AND: Content integrity is maintained
      for record <- fetched do
        assert String.starts_with?(record.value, large_data),
               "Decompressed content should match original despite fragmentation"
      end
    end

    @tag compression_type: :lz4
    @tag :data_limit
    test "lz4 compression works within #{@data_limit_small_bytes}B data limit", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Connection has data limit (closes after limit reached)
      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_small_bytes)

      # AND: Producing small compressed batch (within limit)
      messages = build_test_records(@batch_size_small, "lz4")
      offset = produce_compressed!(ctx.client, topic, messages, 5, :lz4)

      # THEN: Production succeeds (data fits within limit)
      assert_valid_offset(
        offset,
        "lz4 should succeed within #{@data_limit_small_bytes}B data limit"
      )

      # AND: Remove limit for fetch (connection was closed after limit)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "limit_data_downstream")
      Process.sleep(@post_toxic_removal_wait_ms)

      # AND: Messages are retrievable
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
      # GIVEN: A topic for multi-phase chaos testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Phase 1 - High latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      messages_p1 = build_test_records(@batch_size_small, "phase1")
      offset1 = produce_compressed!(ctx.client, topic, messages_p1, 5, :gzip)

      # AND: Phase 2 - Bandwidth throttling
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)
      messages_p2 = build_test_records(@batch_size_small, "phase2")
      offset2 = produce_compressed!(ctx.client, topic, messages_p2, 5, :gzip)

      # AND: Phase 3 - Packet fragmentation
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_slicer(ctx.toxiproxy, ctx.proxy_name, @packet_size_medium_bytes, 50, @packet_delay_ms)
      messages_p3 = build_test_records(@batch_size_small, "phase3")
      offset3 = produce_compressed!(ctx.client, topic, messages_p3, 5, :gzip)

      # THEN: All three phases succeed
      assert_valid_offset(offset1, "Phase 1 (latency) should succeed")
      assert_valid_offset(offset2, "Phase 2 (bandwidth) should succeed")
      assert_valid_offset(offset3, "Phase 3 (fragmentation) should succeed")

      # AND: Offsets show proper progression
      assert_offset_progression(offset1, offset2, "Phase 1 → 2")
      assert_offset_progression(offset2, offset3, "Phase 2 → 3")

      # AND: All messages from all phases are retrievable
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
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Producing highly compressible data with both algorithms
      topic = create_topic(ctx.client, 5)
      compressible = String.duplicate(@highly_compressible_unit, @compressible_reps)

      results =
        for compression <- [:gzip, :snappy] do
          messages = build_test_records(@batch_size_medium, compressible)

          start_time = System.monotonic_time(:millisecond)
          offset = produce_compressed!(ctx.client, topic, messages, 5, compression)
          duration = System.monotonic_time(:millisecond) - start_time

          # Verify success
          assert_valid_offset(
            offset,
            "#{compression} should succeed with #{@moderate_bandwidth_kbps} KB/s limit"
          )

          {compression, duration, offset}
        end

      # THEN: Report compression performance comparison (informational)
      IO.puts("\n[Compression Efficiency Test]")
      IO.puts("Bandwidth: #{@moderate_bandwidth_kbps} KB/s")
      IO.puts("Data: Highly compressible (#{String.length(compressible)} bytes/message)")
      IO.puts("Messages: #{@batch_size_medium}")

      for {compression, duration, _offset} <- results do
        IO.puts("  #{compression}: #{duration}ms")
      end

      # Both compression types should complete successfully
      assert length(results) == 2, "Both gzip and snappy should complete"
    end
  end

  # === Helper Functions ===

  defp build_test_records(count, value_content) do
    for i <- 1..count do
      %Kayrock.RecordBatch.Record{
        value: "#{value_content}-#{i}",
        key: nil,
        headers: []
      }
    end
  end

  defp produce_compressed!(client, topic, records, api_version, compression) do
    # Compression is set via attributes field (bit 0-2)
    compression_attrs =
      case compression do
        :gzip -> 1
        :snappy -> 2
        :lz4 -> 3
        :zstd -> 4
        _ -> 0
      end

    record_batch = %Kayrock.RecordBatch{
      records: records,
      attributes: compression_attrs
    }

    produce_request =
      produce_messages_request(topic, [[record_set: record_batch]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client, produce_request, :controller)

    [topic_response] = response.responses

    assert topic_response.topic == topic,
           "Topic should match"

    [partition_response] = topic_response.partition_responses

    assert partition_response.error_code == 0,
           "Expected error_code=0 for #{compression} compression, got: #{partition_response.error_code}"

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
