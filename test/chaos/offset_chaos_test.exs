defmodule Kayrock.Chaos.OffsetTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @moderate_latency_ms 150
  @high_latency_ms 300
  @very_high_latency_ms 500
  @extreme_timeout_latency_ms 6_000

  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  @connection_drop_duration_ms 10

  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  @timeout_toxic_short_ms 200
  @data_limit_tiny_bytes 30

  @offset_value_small 50
  @offset_value_medium 100
  @offset_value_medium_high 150
  @offset_value_high 200
  @offset_value_large 500
  @offset_value_very_large 777
  @offset_value_max 888
  @offset_value_final 999

  @partition_single 0

  @metadata_size_small "chaos-test"
  @metadata_size_large_bytes 500
  @metadata_size_very_large_bytes 2_500

  @consistency_offset_sequence [100, 200, 300, 400, 500]

  describe "OffsetCommit under network latency" do
    @describetag chaos_type: :latency

    test "commits offset with #{@high_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

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

      [topic_result] = commit_response.topics
      assert topic_result.name == topic

      [partition_result] = topic_result.partitions
      assert partition_result.error_code == 0
    end

    test "commits to multiple partitions with #{@moderate_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

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

      [topic_result] = commit_response.topics
      assert length(topic_result.partitions) == 3

      for partition_result <- topic_result.partitions do
        assert partition_result.error_code == 0
      end
    end
  end

  describe "OffsetCommit under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers from brief connection drop during commit", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-drop-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

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

      disable_proxy(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      enable_proxy(ctx.toxiproxy, ctx.proxy_name)

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

      [topic_result] = commit_response.topics
      [partition_result] = topic_result.partitions
      assert partition_result.error_code == 0

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
      assert partition_result.committed_offset == @offset_value_medium_high
    end
  end

  describe "OffsetCommit under packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    test "commits with #{@packet_size_medium_bytes}B packet fragmentation and large metadata",
         ctx do
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

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

      [topic_result] = commit_response.topics
      [partition_result] = topic_result.partitions
      assert partition_result.error_code == 0
    end
  end

  describe "OffsetFetch under network chaos" do
    @tag chaos_type: :latency
    test "fetches committed offset with #{@very_high_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-latency-#{unique_string()}"
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
      assert partition_result.committed_offset == @offset_value_final
    end

    @tag chaos_type: :connection_failure
    test "recovers from connection drop during fetch", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-drop-#{unique_string()}"
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
                  offset: @offset_value_very_large,
                  metadata: "fetch-test"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      disable_proxy(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      enable_proxy(ctx.toxiproxy, ctx.proxy_name)

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
      assert partition_result.committed_offset == @offset_value_very_large
    end

    @tag chaos_type: :bandwidth
    test "fetches large metadata with #{@moderate_bandwidth_kbps} KB/s bandwidth limit", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

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
      assert partition_result.committed_offset == @offset_value_max
      assert partition_result.metadata == large_metadata
    end
  end

  describe "Offset consistency under chaos" do
    @describetag chaos_type: :consistency

    test "maintains sequential commit semantics under alternating chaos", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-consistency-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      for offset <- @consistency_offset_sequence do
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
        assert partition_result.error_code == 0

        remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      end

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
      assert partition_result.committed_offset == List.last(@consistency_offset_sequence)
    end
  end

  describe "Negative scenarios - offset commit failures" do
    @describetag chaos_type: :negative

    test "fails when #{@timeout_toxic_short_ms}ms timeout closes connection during commit", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

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
                  metadata: "timeout-test"
                ]
              ]
            ]
          ],
          7
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
      end)
    end

    test "fails when connection closes immediately during commit", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-immediate-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

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
                  metadata: "immediate"
                ]
              ]
            ]
          ],
          7
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
      end)
    end

    test "fails when #{@data_limit_tiny_bytes}B data limit truncates commit", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-datalimit-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_tiny_bytes)

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
                  metadata: "data-limit"
                ]
              ]
            ]
          ],
          7
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
      end)
    end

    test "times out with #{@extreme_timeout_latency_ms}ms latency exceeding client timeout",
         ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-offset-extreme-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [partition: @partition_single, offset: @offset_value_large, metadata: "extreme"]
              ]
            ]
          ],
          7
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)
      end)
    end
  end

  describe "Negative scenarios - offset fetch failures" do
    @describetag chaos_type: :negative

    test "fails when #{@timeout_toxic_short_ms}ms timeout closes connection during fetch", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-timeout-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [partition: 0, offset: @offset_value_medium, metadata: "pre-timeout"],
                [partition: 1, offset: @offset_value_high, metadata: "pre-timeout"],
                [partition: 2, offset: @offset_value_high + 100, metadata: "pre-timeout"]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_short_ms)

      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: 0], [partition: 1], [partition: 2]]]],
          6
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
      end)
    end

    test "fails when connection is permanently down during fetch", ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-down-#{unique_string()}"
      coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)

      commit_request =
        offset_commit_request(
          group_id,
          [
            [
              topic: topic,
              partitions: [
                [partition: @partition_single, offset: @offset_value_final, metadata: "down-test"]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      disable_proxy(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)
      Process.sleep(10)

      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
      end)
    end

    test "times out fetch with #{@extreme_timeout_latency_ms}ms latency exceeding client timeout",
         ctx do
      topic = create_topic(ctx.client, 5)
      group_id = "chaos-fetch-extreme-#{unique_string()}"
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
                  offset: @offset_value_max,
                  metadata: "extreme-fetch"
                ]
              ]
            ]
          ],
          7
        )

      {:ok, _} = Kayrock.client_call(ctx.client, commit_request, coordinator.node_id)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic, partitions: [[partition: @partition_single]]]],
          6
        )

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, fetch_request, coordinator.node_id)
      end)
    end
  end
end
