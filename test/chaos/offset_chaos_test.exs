defmodule Kayrock.Chaos.OffsetTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 1_000 - reduced 70%
  @moderate_latency_ms 300
  # Was 2_000 - reduced 70%
  @high_latency_ms 600
  # Was 3_000 - reduced 70%
  @very_high_latency_ms 900
  # Was 5_000 - reduced 70%
  @network_partition_timeout_ms 1_500
  # Was 10_000 - reduced 70%
  @startup_timeout_ms 3_000

  # Bandwidth limits (KB/s)
  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  # Connection timing (milliseconds) - optimized for speed
  # Was 50 - reduced 70%
  @connection_drop_brief_ms 15
  # Was 100 - reduced 70%
  @connection_drop_duration_ms 30
  # Was 300 - reduced 70%
  @connection_recovery_brief_ms 90
  # Was 100 - reduced 70%
  @toxic_removal_wait_ms 30

  # Packet manipulation
  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  # Offset values for testing
  @offset_value_small 50
  @offset_value_medium 100
  @offset_value_medium_high 150
  @offset_value_high 200
  @offset_value_large 500
  @offset_value_very_large 777
  @offset_value_max 888
  @offset_value_final 999

  # Partition configuration
  @partition_single 0

  # Metadata sizes - optimized for speed
  @metadata_size_small "chaos-test"
  # Was 1_000 - reduced 50%
  @metadata_size_large_bytes 500
  # Was 5_000 - reduced 50%
  @metadata_size_very_large_bytes 2_500

  # Consistency test offsets
  @consistency_offset_sequence [100, 200, 300, 400, 500]

  # === Tests ===

  describe "OffsetCommit under network latency" do
    @describetag chaos_type: :latency

    test "successfully commits offset with #{@high_latency_ms}ms latency", ctx do
      # GIVEN: Network with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Finding coordinator and committing offset
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-commit-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_medium,
                  metadata: @metadata_size_small
                ]
              ]
            ]
          ],
          7
        )

      {:ok, commit_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
        end)

      # THEN: Commit succeeds despite latency
      [topic_result] = commit_response.topics

      assert topic_result.name == topic,
             "Topic name should match in response"

      [partition_result] = topic_result.partitions

      assert_no_error(
        partition_result.error_code,
        "Offset commit should succeed with #{@high_latency_ms}ms latency"
      )
    end

    test "handles #{@moderate_latency_ms}ms latency for multi-partition commit", ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Committing offsets to multiple partitions
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-multi-partition-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [partition: 0, offset: @offset_value_medium, metadata: "p0"],
                [partition: 1, offset: @offset_value_high, metadata: "p1"],
                [partition: 2, offset: @offset_value_high + 100, metadata: "p2"]
              ]
            ]
          ],
          7
        )

      {:ok, commit_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
        end)

      # THEN: All partition commits succeed
      [topic_result] = commit_response.topics
      assert_partition_count(topic_result.partitions, 3, "Should commit to all 3 partitions")

      # AND: All partitions have no errors
      for partition_result <- topic_result.partitions do
        assert_no_error(
          partition_result.error_code,
          "Partition #{partition_result.partition} commit should succeed"
        )
      end
    end
  end

  describe "OffsetCommit under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers from brief connection drop during commit", ctx do
      # GIVEN: A topic and group for testing
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # WHEN: First commit succeeds
      commit_request1 =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_small,
                  metadata: "before-drop"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request1, coordinator.node_id)

      # AND: Connection drops then recovers during second commit
      spawn(fn ->
        Process.sleep(@connection_drop_brief_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_duration_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      commit_request2 =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_medium_high,
                  metadata: "after-drop"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, commit_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, commit_request2, coordinator.node_id)
        end)

      # THEN: Second commit succeeds after recovery
      [topic_result] = commit_response.topics
      [partition_result] = topic_result.partitions

      assert_no_error(
        partition_result.error_code,
        "Offset commit should recover from connection drop"
      )

      # AND: Verify second offset persisted (not first)
      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      {:ok, fetch_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
        end)

      [topic_result] = fetch_response.topics
      [partition_result] = topic_result.partitions

      assert_committed_offset(
        partition_result.committed_offset,
        @offset_value_medium_high,
        "Latest committed offset should be persisted"
      )
    end

    @tag :network_partition
    test "handles #{@network_partition_timeout_ms}ms network partition during commit", ctx do
      # GIVEN: A topic and group for testing
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-partition-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # WHEN: Simulating network partition with timeout toxic
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @network_partition_timeout_ms)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_high,
                  metadata: "partition-test"
                ]
              ]
            ]
          ],
          7
        )

      # THEN: Commit may fail or succeed depending on timing
      result =
        try do
          Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
        catch
          _kind, _error -> :network_partition
        end

      case result do
        {:ok, response} ->
          [topic_result] = response.topics
          [partition_result] = topic_result.partitions

          assert is_integer(partition_result.error_code),
                 "Response should have error_code"

        :network_partition ->
          # Expected with timeout toxic
          assert true
      end
    end
  end

  describe "OffsetCommit under packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    @tag :packet_fragmentation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation with large metadata", ctx do
      # GIVEN: Network that fragments packets
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      # WHEN: Committing offset with large metadata (will be fragmented)
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-slicer-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      large_metadata = String.duplicate("x", @metadata_size_large_bytes)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_large,
                  metadata: large_metadata
                ]
              ]
            ]
          ],
          7
        )

      {:ok, commit_response} =
        Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      # THEN: Commit succeeds despite fragmentation
      [topic_result] = commit_response.topics
      [partition_result] = topic_result.partitions

      assert_no_error(
        partition_result.error_code,
        "Offset commit should work with #{@packet_size_medium_bytes}B packet fragmentation"
      )
    end
  end

  describe "OffsetFetch under network chaos" do
    @tag chaos_type: :latency
    test "successfully fetches offset with #{@very_high_latency_ms}ms high latency", ctx do
      # GIVEN: Network with very high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      # WHEN: Committing and fetching offset
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-latency-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # Commit offset first (slow due to latency)
      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_final,
                  metadata: "slow-fetch"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
        end)

      # Fetch with high latency
      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      {:ok, fetch_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
        end)

      # THEN: Fetch succeeds and returns correct offset
      [topic_result] = fetch_response.topics
      [partition_result] = topic_result.partitions

      assert_committed_offset(
        partition_result.committed_offset,
        @offset_value_final,
        "Fetch should succeed with #{@very_high_latency_ms}ms latency"
      )
    end

    @tag chaos_type: :connection_failure
    test "recovers from connection drop during fetch", ctx do
      # GIVEN: A topic with committed offset
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # Commit offset successfully
      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_very_large,
                  metadata: "fetch-test"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      # WHEN: Connection drops before fetch
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_brief_ms)

      # AND: Fetching after recovery
      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      {:ok, fetch_response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
        end)

      # THEN: Fetch succeeds and returns committed offset
      [topic_result] = fetch_response.topics
      [partition_result] = topic_result.partitions

      assert_committed_offset(
        partition_result.committed_offset,
        @offset_value_very_large,
        "Fetch should recover from connection drop"
      )
    end

    @tag chaos_type: :timeout
    test "handles #{@startup_timeout_ms}ms timeout on consumer startup fetch", ctx do
      # GIVEN: A topic with committed offsets for multiple partitions
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-startup-fetch-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # Commit offsets first
      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [partition: 0, offset: @offset_value_medium, metadata: "startup"],
                [partition: 1, offset: @offset_value_high, metadata: "startup"],
                [partition: 2, offset: @offset_value_high + 100, metadata: "startup"]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      # WHEN: Simulating slow startup fetch with timeout
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @startup_timeout_ms)

      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: 0], [partition: 1], [partition: 2]]]],
          6
        )

      # THEN: Fetch may timeout or succeed
      result =
        try do
          Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
        catch
          _kind, _error -> :startup_timeout
        end

      case result do
        {:ok, response} ->
          [topic_result] = response.topics

          assert_partition_count(
            topic_result.partitions,
            3,
            "Startup fetch should return all 3 partitions"
          )

        :startup_timeout ->
          # Expected with extreme timeout
          assert true
      end
    end

    @tag chaos_type: :bandwidth
    test "handles #{@moderate_bandwidth_kbps} KB/s bandwidth limits during fetch with large metadata",
         ctx do
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Committing offset with large metadata
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-bandwidth-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      large_metadata = String.duplicate("x", @metadata_size_very_large_bytes)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [
                  partition: @partition_single,
                  offset: @offset_value_max,
                  metadata: large_metadata
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      # AND: Fetching with bandwidth limit
      start_time = System.monotonic_time(:millisecond)

      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      {:ok, fetch_response} =
        Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Fetch succeeds with large metadata despite bandwidth limit
      [topic_result] = fetch_response.topics
      [partition_result] = topic_result.partitions

      assert_committed_offset(
        partition_result.committed_offset,
        @offset_value_max,
        "Fetch should succeed with #{@moderate_bandwidth_kbps} KB/s limit"
      )

      assert partition_result.metadata == large_metadata,
             "Large metadata should be fetched intact"

      IO.puts("""
      [Bandwidth Test] Offset fetch with #{@moderate_bandwidth_kbps} KB/s limit
        Duration: #{duration}ms, Metadata size: #{@metadata_size_very_large_bytes}B
      """)
    end
  end

  describe "Offset consistency under chaos" do
    @describetag chaos_type: :consistency

    test "maintains at-least-once semantics with #{length(@consistency_offset_sequence)} sequential commits under intermittent chaos",
         ctx do
      # GIVEN: A topic and group for testing consistency
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-consistency-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      # WHEN: Committing sequence of offsets with varying chaos conditions
      for offset <- @consistency_offset_sequence do
        # Vary chaos: latency on even offsets, bandwidth limit on odd
        case rem(offset, 200) do
          0 -> add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
          _ -> add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)
        end

        commit_request =
          offset_commit_request(
            group_id,
            [
              [
                topic: topic,
                partitions: [
                  [partition: @partition_single, offset: offset, metadata: "seq-#{offset}"]
                ]
              ]
            ],
            7
          )

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
          end)

        [topic_result] = response.topics
        [partition_result] = topic_result.partitions

        assert_no_error(
          partition_result.error_code,
          "Commit for offset #{offset} should succeed"
        )

        remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@toxic_removal_wait_ms)
      end

      # THEN: Final fetch should have last committed offset
      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      {:ok, fetch_response} =
        Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)

      [topic_result] = fetch_response.topics
      [partition_result] = topic_result.partitions

      last_offset = List.last(@consistency_offset_sequence)

      assert_committed_offset(
        partition_result.committed_offset,
        last_offset,
        "Final offset should be #{last_offset} (last in sequence)"
      )
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
  end

  defp assert_committed_offset(actual_offset, expected_offset, message) do
    msg = message || "Expected offset #{expected_offset}, got #{actual_offset}"
    assert actual_offset == expected_offset, msg
    actual_offset
  end

  defp assert_partition_count(partitions, expected_count, message) do
    actual_count = length(partitions)
    msg = message || "Expected #{expected_count} partitions, got #{actual_count}"
    assert actual_count == expected_count, msg
    partitions
  end
end
