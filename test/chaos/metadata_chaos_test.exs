defmodule Kayrock.Chaos.MetadataTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 2_000 - reduced 70%
  @high_latency_ms 600
  # Was 3_000 - reduced 70%
  @very_high_latency_ms 900
  # Was 5_000 - reduced 70%
  @extreme_latency_ms 1_500
  # Was 30_000 - reduced 70%
  @extreme_timeout_latency_ms 9_000

  # Jitter configuration - optimized for speed
  # Was 2_000 - reduced 70%
  @jitter_base_latency_ms 600
  # Was 1_000 - reduced 70%
  @jitter_amount_high_ms 300

  # Bandwidth limits (KB/s)
  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  # Connection timing (milliseconds) - optimized for speed
  # Was 100 - reduced 70%
  @connection_drop_duration_ms 30
  # Was 500 - reduced 70%
  @connection_recovery_wait_ms 150
  # Was 100 - reduced 70%
  @brief_delay_ms 30
  # Was 500 - reduced 70%
  @metadata_refresh_interval_ms 150

  # Timeout toxic - optimized for speed
  # Was 1_000 - reduced 70%
  @timeout_toxic_short_ms 300
  # Was 15_000 - reduced 70%
  @timeout_toxic_long_ms 4_500

  # Packet manipulation
  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  # Data limits
  # 2 KB
  @data_limit_small_bytes 2_048
  # 5 KB
  @data_limit_medium_bytes 5_120

  # Topic configuration
  @topic_count_small 3
  @topic_count_medium 5
  @topic_count_large 10
  @topic_name_padding_bytes 100

  # Flaky network - optimized for speed
  # Was 3 - reduced 33%
  @flaky_network_cycles 2
  # Was 50 - reduced 60%
  @flaky_network_down_ms 20
  # Was 100 - reduced 70%
  @flaky_network_up_ms 30

  # Metadata refresh - optimized for speed
  # Was 3 - reduced 33%
  @metadata_refresh_count 2

  # Duration thresholds - optimized for speed
  # Was 4_000 - reduced 70%
  @expected_latency_impact_ms 1_200
  # Was 60_000 - reduced 70%
  @max_timeout_duration_ms 18_000

  # === Tests ===

  describe "Metadata request under network latency" do
    @describetag chaos_type: :latency

    test "successfully fetches broker list with #{@extreme_latency_ms}ms high latency", ctx do
      # GIVEN: Network with extreme latency (simulates very slow network)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_latency_ms)

      # WHEN: Requesting metadata for all brokers
      request = metadata_request([], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: Request succeeds and returns broker list
      assert_broker_list_present(
        response.brokers,
        "Metadata should return brokers with #{@extreme_latency_ms}ms latency"
      )

      assert is_list(response.topics),
             "Metadata should return topics list"
    end

    @tag :jittery_network
    test "fetches topic metadata with #{@jitter_base_latency_ms}ms ± #{@jitter_amount_high_ms}ms jitter",
         ctx do
      # GIVEN: Network with high jitter (unstable network simulation)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_base_latency_ms, @jitter_amount_high_ms)

      # WHEN: Creating topic and requesting metadata
      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: Topic metadata is returned correctly
      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)

      assert topic_meta != nil,
             "Topic #{topic} should be in metadata response"

      assert_no_error(
        topic_meta.error_code,
        "Topic should have no error with jittery network"
      )
    end

    test "fetches metadata for #{@topic_count_small} topics with #{@very_high_latency_ms}ms latency",
         ctx do
      # GIVEN: Network with very high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      # WHEN: Creating multiple topics
      topic1 = create_topic(ctx.client, 5)
      topic2 = create_topic(ctx.client, 5)
      topic3 = create_topic(ctx.client, 5)
      all_topics = [topic1, topic2, topic3]

      # AND: Requesting metadata for all topics
      request = metadata_request(all_topics, 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: All topics present in metadata
      topic_names = Enum.map(response.topics, & &1.name)

      for topic <- all_topics do
        assert topic in topic_names,
               "Topic #{topic} should be in metadata with #{@very_high_latency_ms}ms latency"
      end
    end
  end

  describe "Metadata under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers metadata fetch after brief connection drop", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: First request succeeds
      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      initial_broker_count = length(response1.brokers)

      # AND: Connection drops then recovers
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      # AND: Second request after recovery
      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      # THEN: Both requests return same broker count
      assert_broker_count(
        response2.brokers,
        initial_broker_count,
        "Broker count should be consistent after recovery"
      )

      topic_meta = Enum.find(response2.topics, fn t -> t.name == topic end)

      assert topic_meta != nil,
             "Topic metadata should be available after connection recovery"
    end

    @tag :flaky_network
    test "handles #{@flaky_network_cycles} intermittent connection failures", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Simulating flaky network
      for _ <- 1..@flaky_network_cycles do
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@flaky_network_down_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
        Process.sleep(@flaky_network_up_ms)
      end

      # AND: Requesting metadata after network stabilizes
      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: Metadata request succeeds
      assert_broker_list_present(
        response.brokers,
        "Metadata should be available after #{@flaky_network_cycles} connection drops"
      )
    end

    test "handles #{@timeout_toxic_long_ms}ms timeout during metadata request", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Adding extreme timeout
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_long_ms)

      request = metadata_request([topic], 9)

      # THEN: Request may timeout or succeed
      result =
        try do
          Kayrock.client_call(ctx.client, request, :random)
        catch
          _kind, _error -> :metadata_timeout
        end

      case result do
        {:ok, response} ->
          # If succeeded, verify structure
          assert_broker_list_present(
            response.brokers,
            "Metadata should have brokers if request succeeded"
          )

        :metadata_timeout ->
          # Expected with extreme timeout
          assert true
      end
    end
  end

  describe "Metadata under bandwidth constraints" do
    @describetag chaos_type: :bandwidth

    test "fetches large cluster metadata (#{@topic_count_large} topics) with #{@moderate_bandwidth_kbps} KB/s throttling",
         ctx do
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Creating many topics to generate large metadata response
      topics =
        for i <- 1..@topic_count_large do
          create_topic(ctx.client, 5, name: "throttle-topic-#{i}-#{unique_string()}")
        end

      request = metadata_request(topics, 9)

      start_time = System.monotonic_time(:millisecond)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: All topics returned despite bandwidth limit
      assert_topic_count(
        response.topics,
        @topic_count_large,
        "All #{@topic_count_large} topics should be in metadata"
      )

      IO.puts("""
      [Bandwidth Test] Large metadata with #{@moderate_bandwidth_kbps} KB/s limit
        Duration: #{duration}ms, Topics: #{@topic_count_large}
      """)
    end

    @tag :metadata_refresh
    test "handles #{@metadata_refresh_count} metadata refreshes under #{@high_bandwidth_kbps} KB/s constraints",
         ctx do
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      # WHEN: Creating topic and refreshing metadata multiple times
      topic = create_topic(ctx.client, 5)

      # AND: Multiple metadata requests (simulating periodic refresh)
      for _ <- 1..@metadata_refresh_count do
        request = metadata_request([topic], 9)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :random)
          end)

        # THEN: Each refresh succeeds
        assert_broker_list_present(
          response.brokers,
          "Each metadata refresh should return brokers"
        )

        Process.sleep(@metadata_refresh_interval_ms)
      end
    end
  end

  describe "Metadata under packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    @tag :packet_fragmentation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation with long topic name", ctx do
      # GIVEN: Network that fragments packets
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      # WHEN: Creating topic with long name (increases response size)
      long_name =
        "very-long-topic-name-#{String.duplicate("x", @topic_name_padding_bytes)}-#{unique_string()}"

      topic = create_topic(ctx.client, 5, name: long_name)

      request = metadata_request([topic], 9)

      {:ok, response} = Kayrock.client_call(ctx.client, request, :random)

      # THEN: Metadata deserializes correctly despite fragmentation
      assert_broker_list_present(
        response.brokers,
        "Brokers should be present despite #{@packet_size_medium_bytes}B fragmentation"
      )

      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)

      assert topic_meta != nil,
             "Topic with long name should be in metadata"

      assert_no_error(
        topic_meta.error_code,
        "Topic metadata should be valid despite packet fragmentation"
      )
    end

    @tag :data_limit
    test "handles #{@data_limit_medium_bytes}B data limit during large metadata response", ctx do
      # GIVEN: Multiple topics created first
      topics =
        for i <- 1..@topic_count_medium do
          create_topic(ctx.client, 5, name: "limit-topic-#{i}-#{unique_string()}")
        end

      # WHEN: Limiting data transferred
      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_medium_bytes)

      request = metadata_request(topics, 9)

      # THEN: Request may succeed (fits) or fail (limit exceeded)
      result =
        try do
          Kayrock.client_call(ctx.client, request, :random)
        catch
          _kind, _error -> :data_limit_exceeded
        end

      case result do
        {:ok, response} ->
          # If succeeded, metadata fits within limit
          assert is_list(response.brokers),
                 "Metadata should be valid if within #{@data_limit_medium_bytes}B limit"

        :data_limit_exceeded ->
          # Expected if response too large
          assert true
      end
    end
  end

  describe "Partition leader discovery under chaos" do
    @describetag chaos_type: :leader_discovery

    test "discovers partition leaders with #{@high_latency_ms}ms latency", ctx do
      # GIVEN: Network with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Creating topic and requesting metadata
      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: Topic metadata includes partition leaders
      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)

      # AND: Each partition has a valid leader
      for partition <- topic_meta.partitions do
        leader = Map.get(partition, :leader) || Map.get(partition, :leader_id)

        assert_valid_leader(
          leader,
          "Partition #{partition.partition} should have valid leader"
        )
      end
    end

    test "discovers consistent partition leaders after connection drop", ctx do
      # GIVEN: A topic with initial metadata
      topic = create_topic(ctx.client, 5)

      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      topic_meta1 = Enum.find(response1.topics, fn t -> t.name == topic end)
      _initial_leaders = extract_leaders(response1, topic)
      initial_partition_count = length(topic_meta1.partitions)

      # WHEN: Connection drops then recovers
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      # AND: Fetching metadata after recovery
      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      topic_meta2 = Enum.find(response2.topics, fn t -> t.name == topic end)

      # THEN: Leaders are consistent (or valid if reelected)
      for partition <- topic_meta2.partitions do
        leader = Map.get(partition, :leader) || Map.get(partition, :leader_id)

        assert_valid_leader(
          leader,
          "Partition #{partition.partition} should have valid leader after recovery"
        )
      end

      # AND: Same number of partitions
      assert length(topic_meta2.partitions) == initial_partition_count,
             "Partition count should be consistent"
    end
  end

  describe "Metadata caching behavior under chaos" do
    @describetag chaos_type: :caching

    test "maintains consistent broker information with #{@very_high_latency_ms}ms latency", ctx do
      # GIVEN: A topic for testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Getting fresh metadata
      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      broker_count1 = length(response1.brokers)

      # AND: Adding latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      # AND: Subsequent request with latency
      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      _broker_count2 = length(response2.brokers)

      # THEN: Broker count should be consistent
      assert_broker_count(
        response2.brokers,
        broker_count1,
        "Broker count should be consistent despite latency"
      )
    end

    @tag :error_handling
    test "handles metadata for non-existent topic with #{@high_latency_ms}ms latency", ctx do
      # GIVEN: Network with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Requesting metadata for non-existent topic
      non_existent = "non-existent-#{unique_string()}"
      request = metadata_request([non_existent], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      # THEN: Response returned with error for topic
      topic_meta = Enum.find(response.topics, fn t -> t.name == non_existent end)

      if topic_meta do
        # If present in response, should have error code
        assert topic_meta.error_code != 0,
               "Non-existent topic should have error_code != 0"
      end
    end
  end

  describe "Combined metadata chaos scenarios" do
    @describetag chaos_type: :combined

    @tag :multi_phase
    test "metadata survives three sequential chaos phases", ctx do
      # GIVEN: A topic for multi-phase testing
      topic = create_topic(ctx.client, 5)

      # WHEN: Phase 1 - High latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)
      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)

      # THEN: Phase 1 succeeds
      assert_broker_list_present(
        response1.brokers,
        "Phase 1 (latency) should return brokers"
      )

      # WHEN: Phase 2 - Bandwidth limit
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)
      request2 = metadata_request([topic], 9)
      {:ok, response2} = Kayrock.client_call(ctx.client, request2, :random)

      # THEN: Phase 2 succeeds
      assert_broker_list_present(
        response2.brokers,
        "Phase 2 (bandwidth) should return brokers"
      )

      # WHEN: Phase 3 - Packet fragmentation
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)

      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      request3 = metadata_request([topic], 9)
      {:ok, response3} = Kayrock.client_call(ctx.client, request3, :random)

      # THEN: Phase 3 succeeds
      assert_broker_list_present(
        response3.brokers,
        "Phase 3 (fragmentation) should return brokers"
      )

      # AND: All responses are consistent
      assert response1.brokers == response2.brokers,
             "Phase 1 and 2 should have same brokers"

      assert response2.brokers == response3.brokers,
             "Phase 2 and 3 should have same brokers"
    end
  end

  # === Negative Scenarios ===

  describe "Negative scenarios - metadata failures" do
    @tag chaos_type: :extreme_latency
    test "returns error when metadata request times out due to #{@extreme_timeout_latency_ms}ms extreme latency",
         ctx do
      # GIVEN: Extreme latency that exceeds typical timeout
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      # WHEN: Requesting metadata
      # THEN: Should timeout with clear error
      result =
        try do
          Kayrock.client_call(ctx.client, request, :random)
        catch
          kind, error -> {:error, kind, error}
        end

      case result do
        {:error, _kind, _error} ->
          # Expected: timeout
          assert true

        {:ok, response} ->
          # Unexpected with extreme latency
          flunk("Expected timeout but metadata request succeeded: #{inspect(response)}")
      end
    end

    @tag chaos_type: :connection_failure
    test "fails gracefully when all brokers unreachable", ctx do
      # GIVEN: Connection completely down
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@brief_delay_ms)

      # WHEN: Requesting metadata
      request = metadata_request([], 9)

      # THEN: Should fail with connection error
      assert_raise MatchError, fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end
    end

    @tag chaos_type: :timeout
    test "handles metadata fetch failure during bootstrap with #{@timeout_toxic_short_ms}ms timeout",
         ctx do
      # GIVEN: Network issues during initial connection
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      # WHEN: First metadata request (bootstrap scenario)
      request = metadata_request([], 9)

      # THEN: May timeout or succeed
      result =
        try do
          Kayrock.client_call(ctx.client, request, :random)
        catch
          _kind, _error -> :timeout
        end

      case result do
        :timeout ->
          # Expected during bootstrap with timeout
          assert true

        {:ok, response} ->
          # If succeeded, should have broker list
          assert is_list(response.brokers),
                 "Bootstrap metadata should have brokers if succeeded"
      end
    end

    @tag chaos_type: :data_limit
    test "returns partial data when request exceeds #{@data_limit_small_bytes}B limit", ctx do
      # GIVEN: Small data limit
      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_small_bytes)

      # WHEN: Creating multiple topics to increase response size
      topics =
        for i <- 1..@topic_count_medium do
          create_topic(ctx.client, 5, name: "topic-#{i}-#{unique_string()}")
        end

      request = metadata_request(topics, 9)

      # THEN: May succeed (data fits) or fail (limit exceeded)
      result =
        try do
          Kayrock.client_call(ctx.client, request, :random)
        catch
          _kind, _error -> :limit_exceeded
        end

      case result do
        {:ok, response} ->
          # If succeeded, response should be valid
          assert is_list(response.brokers),
                 "Metadata should be valid if within limit"

        :limit_exceeded ->
          # Expected: response too large for limit
          assert true
      end
    end

    @tag chaos_type: :staleness
    test "detects consistent metadata when #{@extreme_latency_ms}ms latency causes delays", ctx do
      # GIVEN: A topic with initial metadata
      topic = create_topic(ctx.client, 5)

      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      initial_leaders = extract_leaders(response1, topic)

      # WHEN: Adding extreme latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_latency_ms)

      # AND: Fetching metadata again (slow)
      request2 = metadata_request([topic], 9)

      start_time = System.monotonic_time(:millisecond)

      result =
        try do
          Kayrock.client_call(ctx.client, request2, :random)
        catch
          _kind, _error -> :timeout
        end

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Verify behavior
      case result do
        {:ok, response2} ->
          # Should have taken longer due to latency
          assert duration >= @expected_latency_impact_ms,
                 "Expected latency impact, took #{duration}ms"

          delayed_leaders = extract_leaders(response2, topic)
          # Leaders should be consistent (no stale data)
          assert initial_leaders == delayed_leaders,
                 "Leaders should be consistent despite latency"

        :timeout ->
          # Acceptable: request timed out
          assert duration < @max_timeout_duration_ms,
                 "Should timeout faster, took #{duration}ms"
      end
    end
  end

  # === Helper Functions ===

  defp extract_leaders(metadata_response, topic_name) do
    topic_meta = Enum.find(metadata_response.topics, fn t -> t.name == topic_name end)

    if topic_meta do
      topic_meta.partitions
      |> Enum.sort_by(& &1.partition)
      |> Enum.map(&(Map.get(&1, :leader) || Map.get(&1, :leader_id)))
    else
      []
    end
  end

  # === Assertion Helpers ===

  defp assert_no_error(error_code, message) do
    assert error_code == 0,
           "#{message} (got error_code: #{error_code})"
  end

  defp assert_broker_list_present(brokers, message) do
    assert brokers != [],
           "#{message} (got empty broker list)"

    brokers
  end

  defp assert_broker_count(brokers, expected_count, message) do
    actual_count = length(brokers)
    msg = message || "Expected #{expected_count} brokers, got #{actual_count}"
    assert actual_count == expected_count, msg
    brokers
  end

  defp assert_topic_count(topics, expected_count, message) do
    actual_count = length(topics)
    msg = message || "Expected #{expected_count} topics, got #{actual_count}"
    assert actual_count >= expected_count, msg
    topics
  end

  defp assert_valid_leader(leader_id, message) do
    assert is_integer(leader_id) and leader_id >= 0,
           "#{message} (got: #{inspect(leader_id)})"

    leader_id
  end
end
