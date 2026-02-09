defmodule Kayrock.Chaos.MetadataTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @high_latency_ms 300
  @very_high_latency_ms 500
  @extreme_latency_ms 800
  @extreme_timeout_latency_ms 6_000

  @jitter_base_latency_ms 300
  @jitter_amount_high_ms 150

  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  @connection_drop_duration_ms 10
  @connection_recovery_wait_ms 30
  @brief_delay_ms 10
  @metadata_refresh_interval_ms 50

  @timeout_toxic_ms 300

  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  @data_limit_tiny_bytes 50

  @topic_count_small 3
  @topic_count_large 10
  @topic_name_padding_bytes 100

  @flaky_network_cycles 2
  @flaky_network_down_ms 5
  @flaky_network_up_ms 15

  @metadata_refresh_count 2

  describe "Metadata request under network latency" do
    @describetag chaos_type: :latency

    test "successfully fetches broker list with #{@extreme_latency_ms}ms high latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_latency_ms)

      request = metadata_request([], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      assert response.brokers != []
      assert is_list(response.topics)
    end

    @tag :jittery_network
    test "fetches topic metadata with #{@jitter_base_latency_ms}ms +/- #{@jitter_amount_high_ms}ms jitter",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @jitter_base_latency_ms, @jitter_amount_high_ms)

      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)

      assert topic_meta != nil, "Topic #{topic} should be in metadata response"
      assert topic_meta.error_code == 0
    end

    test "fetches metadata for #{@topic_count_small} topics with #{@very_high_latency_ms}ms latency",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      topic1 = create_topic(ctx.client, 5)
      topic2 = create_topic(ctx.client, 5)
      topic3 = create_topic(ctx.client, 5)
      all_topics = [topic1, topic2, topic3]

      request = metadata_request(all_topics, 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

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
      topic = create_topic(ctx.client, 5)

      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      initial_broker_count = length(response1.brokers)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      assert length(response2.brokers) == initial_broker_count

      topic_meta = Enum.find(response2.topics, fn t -> t.name == topic end)
      assert topic_meta != nil
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

      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      assert response.brokers != []
    end
  end

  describe "Metadata under bandwidth constraints" do
    @describetag chaos_type: :bandwidth

    test "fetches large cluster metadata (#{@topic_count_large} topics) with #{@moderate_bandwidth_kbps} KB/s throttling",
         ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topics =
        for i <- 1..@topic_count_large do
          create_topic(ctx.client, 5, name: "throttle-topic-#{i}-#{unique_string()}")
        end

      request = metadata_request(topics, 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      assert length(response.topics) >= @topic_count_large
    end

    @tag :metadata_refresh
    test "handles #{@metadata_refresh_count} metadata refreshes under #{@high_bandwidth_kbps} KB/s constraints",
         ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      topic = create_topic(ctx.client, 5)

      for _ <- 1..@metadata_refresh_count do
        request = metadata_request([topic], 9)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :random)
          end)

        assert response.brokers != []

        Process.sleep(@metadata_refresh_interval_ms)
      end
    end
  end

  describe "Metadata under packet manipulation" do
    @describetag chaos_type: :packet_manipulation

    @tag :packet_fragmentation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation with long topic name", ctx do
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      long_name =
        "very-long-topic-name-#{String.duplicate("x", @topic_name_padding_bytes)}-#{unique_string()}"

      topic = create_topic(ctx.client, 5, name: long_name)

      request = metadata_request([topic], 9)

      {:ok, response} = Kayrock.client_call(ctx.client, request, :random)

      assert response.brokers != []

      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)
      assert topic_meta != nil
      assert topic_meta.error_code == 0
    end
  end

  describe "Partition leader discovery under chaos" do
    @describetag chaos_type: :leader_discovery

    test "discovers partition leaders with #{@high_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      topic_meta = Enum.find(response.topics, fn t -> t.name == topic end)

      for partition <- topic_meta.partitions do
        leader = partition.leader_id

        assert is_integer(leader) and leader >= 0,
               "Partition #{partition.partition_index} should have valid leader (got: #{inspect(leader)})"
      end
    end

    test "discovers consistent partition leaders after connection drop", ctx do
      topic = create_topic(ctx.client, 5)

      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      topic_meta1 = Enum.find(response1.topics, fn t -> t.name == topic end)
      initial_partition_count = length(topic_meta1.partitions)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      topic_meta2 = Enum.find(response2.topics, fn t -> t.name == topic end)

      for partition <- topic_meta2.partitions do
        leader = partition.leader_id

        assert is_integer(leader) and leader >= 0,
               "Partition #{partition.partition_index} should have valid leader after recovery (got: #{inspect(leader)})"
      end

      assert length(topic_meta2.partitions) == initial_partition_count
    end
  end

  describe "Metadata caching behavior under chaos" do
    @describetag chaos_type: :caching

    test "maintains consistent broker information with #{@very_high_latency_ms}ms latency", ctx do
      topic = create_topic(ctx.client, 5)

      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      broker_count1 = length(response1.brokers)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      request2 = metadata_request([topic], 9)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request2, :random)
        end)

      assert length(response2.brokers) == broker_count1
    end

    @tag :error_handling
    test "handles metadata for non-existent topic with #{@high_latency_ms}ms latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      non_existent = "non-existent-#{unique_string()}"
      request = metadata_request([non_existent], 9)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :random)
        end)

      topic_meta = Enum.find(response.topics, fn t -> t.name == non_existent end)

      if topic_meta != nil do
        assert topic_meta.error_code != 0,
               "Non-existent topic should have error_code != 0, got: #{topic_meta.error_code}"
      end
    end
  end

  describe "Combined metadata chaos scenarios" do
    @describetag chaos_type: :combined

    @tag :multi_phase
    test "metadata survives three sequential chaos phases", ctx do
      topic = create_topic(ctx.client, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)
      request1 = metadata_request([topic], 9)
      {:ok, response1} = Kayrock.client_call(ctx.client, request1, :random)
      assert response1.brokers != []

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)
      request2 = metadata_request([topic], 9)
      {:ok, response2} = Kayrock.client_call(ctx.client, request2, :random)
      assert response2.brokers != []

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
      assert response3.brokers != []

      assert response1.brokers == response2.brokers
      assert response2.brokers == response3.brokers
    end
  end

  describe "Negative scenarios - metadata failures" do
    @describetag chaos_type: :negative

    test "fails when #{@timeout_toxic_ms}ms timeout closes connection during metadata",
         ctx do
      topic = create_topic(ctx.client, 5)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      request = metadata_request([topic], 9)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end

    test "fails when all brokers unreachable", ctx do
      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)
      Process.sleep(@brief_delay_ms)

      request = metadata_request([], 9)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end

    test "fails when #{@timeout_toxic_ms}ms timeout closes connection during bootstrap",
         ctx do
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      request = metadata_request([], 9)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end

    test "fails when connection closes immediately (0ms timeout)", ctx do
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      request = metadata_request([], 9)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end

    test "times out with #{@extreme_timeout_latency_ms}ms extreme latency", ctx do
      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end

    test "fails when data limit is too small (#{@data_limit_tiny_bytes}B)", ctx do
      topic = create_topic(ctx.client, 5)
      request = metadata_request([topic], 9)

      add_limit_data(ctx.toxiproxy, ctx.proxy_name, @data_limit_tiny_bytes)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :random)
      end)
    end
  end
end
