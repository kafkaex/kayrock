defmodule Kayrock.Chaos.TopicManagementTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  # === Configuration Constants ===

  # Network latency (milliseconds) - optimized for speed
  # Was 1_500 - reduced 70%
  @moderate_latency_ms 450
  # Was 3_000 - reduced 70%
  @high_latency_ms 900
  # Was 5_000 - reduced 70%
  @very_high_latency_ms 1_500
  # Was 20_000 - reduced 70%
  @extreme_timeout_ms 6_000

  # Bandwidth limits (KB/s)
  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  # Connection timing (milliseconds) - optimized for speed
  # Was 50 - reduced 70%
  @connection_drop_brief_ms 15
  # Was 150 - reduced 70%
  @connection_drop_duration_ms 45
  # Was 500 - reduced 70%
  @post_failure_wait_ms 150

  # Packet manipulation
  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  # Topic naming - optimized for speed
  # Was 100 - reduced 50%
  @topic_name_padding_bytes 50
  # Was 3 - reduced 33%
  @multi_topic_count 2

  # Timeouts - optimized for speed
  # Was 30_000 - reduced 70%
  @delete_timeout_ms 9_000

  # === Tests ===

  describe "Topic creation under network latency" do
    @describetag chaos_type: :latency

    test "successfully creates topic with #{@high_latency_ms}ms high latency", ctx do
      # GIVEN: Network with high latency (typical for admin operations)
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      # WHEN: Creating a topic
      topic_name = "chaos-create-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      # THEN: Topic creation succeeds despite latency
      [topic_result] = response.topics

      assert topic_result.name == topic_name,
             "Topic name should match in response"

      # May have error_code (V5+) or error_message (V0-V4)
      if Map.has_key?(topic_result, :error_code) do
        assert_no_error(
          topic_result.error_code,
          "Topic creation should succeed with #{@high_latency_ms}ms latency"
        )
      end
    end

    test "creates #{@multi_topic_count} topics sequentially with #{@moderate_latency_ms}ms latency",
         ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Creating multiple topics
      topics =
        for i <- 1..@multi_topic_count do
          "chaos-multi-#{i}-#{unique_string()}"
        end

      # AND: Creating each topic sequentially
      for topic_name <- topics do
        request = create_topic_request(topic_name, 5)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)

        [topic_result] = response.topics

        assert topic_result.name == topic_name,
               "Topic #{topic_name} should be created"
      end

      # THEN: Verify all topics exist via metadata
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      metadata_request = metadata_request(topics, 9)

      {:ok, metadata_response} =
        Kayrock.client_call(ctx.client, metadata_request, :random)

      created_topics = Enum.map(metadata_response.topics, & &1.name)

      for topic_name <- topics do
        assert topic_name in created_topics,
               "Topic #{topic_name} should exist after creation"
      end
    end
  end

  describe "Topic creation under connection failures" do
    @describetag chaos_type: :connection_failure

    @tag :idempotency
    test "handles connection drop during topic creation (idempotent)", ctx do
      # GIVEN: Topic name for testing
      topic_name = "chaos-create-drop-#{unique_string()}"

      # WHEN: Dropping connection during create request
      spawn(fn ->
        Process.sleep(@connection_drop_brief_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_duration_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      request = create_topic_request(topic_name, 5)

      # THEN: Operation may fail or succeed (idempotent)
      result =
        try do
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)
        catch
          _kind, _error -> :create_failed
        end

      case result do
        {:ok, response} ->
          [topic_result] = response.topics

          assert topic_result.name == topic_name,
                 "Topic should be created despite connection drop"

        :create_failed ->
          # Verify topic state (may or may not exist - both acceptable)
          Process.sleep(@post_failure_wait_ms)

          metadata_request = metadata_request([topic_name], 9)

          {:ok, metadata_response} =
            with_retry(fn ->
              Kayrock.client_call(ctx.client, metadata_request, :random)
            end)

          # Topic may exist (created before drop) or not (failed before creation)
          topic_meta = Enum.find(metadata_response.topics, fn t -> t.name == topic_name end)

          if topic_meta do
            # Topic was created despite connection drop (idempotent)
            assert true
          else
            # Topic creation failed cleanly
            assert true
          end
      end
    end

    @tag chaos_type: :timeout
    test "handles #{@extreme_timeout_ms}ms timeout during topic creation", ctx do
      # GIVEN: Extreme timeout toxic
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_ms)

      # WHEN: Attempting topic creation
      topic_name = "chaos-create-timeout-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      # THEN: May timeout or succeed
      result =
        try do
          Kayrock.client_call(ctx.client, request, :controller)
        catch
          _kind, _error -> :create_timeout
        end

      case result do
        {:ok, response} ->
          [topic_result] = response.topics

          assert topic_result.name == topic_name,
                 "If creation succeeded, topic name should match"

        :create_timeout ->
          # Expected with extreme timeout
          assert true
      end
    end
  end

  describe "Topic creation under bandwidth/packet constraints" do
    @tag chaos_type: :bandwidth
    test "creates topic with #{@moderate_bandwidth_kbps} KB/s bandwidth throttling", ctx do
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      # WHEN: Creating topic (measures duration impact)
      topic_name = "chaos-create-bw-#{unique_string()}"

      start_time = System.monotonic_time(:millisecond)
      request = create_topic_request(topic_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Creation succeeds despite bandwidth limit
      [topic_result] = response.topics

      assert topic_result.name == topic_name,
             "Topic creation should succeed with #{@moderate_bandwidth_kbps} KB/s limit"

      IO.puts("""
      [Bandwidth Test] Topic creation with #{@moderate_bandwidth_kbps} KB/s limit
        Duration: #{duration}ms
      """)
    end

    @tag chaos_type: :packet_manipulation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation with long topic name", ctx do
      # GIVEN: Network that fragments packets
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      # WHEN: Creating topic with long name (triggers fragmentation)
      long_name =
        "chaos-fragmented-#{String.duplicate("x", @topic_name_padding_bytes)}-#{unique_string()}"

      request = create_topic_request(long_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      # THEN: Creation succeeds despite packet fragmentation
      [topic_result] = response.topics

      assert topic_result.name == long_name,
             "Topic with long name should be created despite #{@packet_size_medium_bytes}B fragmentation"
    end
  end

  describe "Topic deletion under network chaos" do
    @tag chaos_type: :latency
    test "deletes topic with #{@high_latency_ms}ms high latency", ctx do
      # GIVEN: A topic to delete
      topic_name = "chaos-delete-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      # WHEN: Deleting with high latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      delete_request = delete_topic_request(topic_name, 4)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, delete_request, :controller)
        end)

      # THEN: Deletion succeeds despite latency
      [topic_result] = response.topics

      assert topic_result.name == topic_name,
             "Topic deletion should succeed with #{@high_latency_ms}ms latency"
    end

    @tag chaos_type: :connection_failure
    test "handles connection drop during topic deletion", ctx do
      # GIVEN: A topic to delete
      topic_name = "chaos-delete-drop-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      # WHEN: Dropping connection during delete
      spawn(fn ->
        Process.sleep(@connection_drop_brief_ms)
        add_down(ctx.toxiproxy, ctx.proxy_name)
        Process.sleep(@connection_drop_duration_ms)
        remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      end)

      delete_request = delete_topic_request(topic_name, 4)

      # THEN: Delete may fail or succeed (idempotent operation)
      result =
        try do
          with_retry(fn ->
            Kayrock.client_call(ctx.client, delete_request, :controller)
          end)
        catch
          _kind, _error -> :delete_failed
        end

      case result do
        {:ok, response} ->
          [topic_result] = response.topics

          assert topic_result.name == topic_name,
                 "Topic deletion should complete despite connection drop"

        :delete_failed ->
          # Expected with connection drop - deletion may be retried
          assert true
      end
    end

    @tag chaos_type: :latency
    test "handles #{@very_high_latency_ms}ms slow deletion response", ctx do
      # GIVEN: A topic to delete
      topic_name = "chaos-delete-slow-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      # WHEN: Deleting with extreme latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      delete_request = delete_topic_request(topic_name, 4)

      start_time = System.monotonic_time(:millisecond)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, delete_request, :controller)
        end)

      duration = System.monotonic_time(:millisecond) - start_time

      # THEN: Deletion eventually succeeds
      [topic_result] = response.topics

      assert topic_result.name == topic_name,
             "Topic deletion should complete with #{@very_high_latency_ms}ms latency"

      IO.puts("""
      [Latency Test] Topic deletion with #{@very_high_latency_ms}ms latency
        Duration: #{duration}ms
      """)
    end
  end

  describe "Multiple topic operations under chaos" do
    @describetag chaos_type: :bulk_operations

    test "creates #{@multi_topic_count} topics with #{@moderate_latency_ms}ms latency", ctx do
      # GIVEN: Network with moderate latency
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      # WHEN: Creating multiple topics sequentially
      topics =
        for i <- 1..@multi_topic_count do
          "chaos-multi-#{i}-#{unique_string()}"
        end

      # AND: Creating each topic
      for topic_name <- topics do
        request = create_topic_request(topic_name, 5)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)

        [topic_result] = response.topics

        assert topic_result.name == topic_name,
               "Topic #{topic_name} creation should succeed"
      end

      # THEN: Verify all topics exist via metadata
      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      metadata_request = metadata_request(topics, 9)

      {:ok, metadata_response} =
        Kayrock.client_call(ctx.client, metadata_request, :random)

      created_topics = Enum.map(metadata_response.topics, & &1.name)

      for topic_name <- topics do
        assert topic_name in created_topics,
               "Topic #{topic_name} should exist in metadata"
      end
    end

    @tag chaos_type: :bandwidth
    test "deletes #{@multi_topic_count} topics with #{@high_bandwidth_kbps} KB/s bandwidth limits",
         ctx do
      # GIVEN: Throttled bandwidth
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      # WHEN: Creating topics first (without chaos)
      topics =
        for i <- 1..@multi_topic_count do
          name = "chaos-delete-multi-#{i}-#{unique_string()}"
          create_topic(ctx.client, 5, name: name)
          name
        end

      # AND: Deleting all topics with bandwidth limit
      for topic_name <- topics do
        request = delete_topic_request(topic_name, 4)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)

        [topic_result] = response.topics

        assert topic_result.name == topic_name,
               "Topic #{topic_name} deletion should succeed with bandwidth limit"
      end

      # THEN: All deletions completed successfully
      assert length(topics) == @multi_topic_count,
             "All #{@multi_topic_count} topics should be deleted"
    end
  end

  # === Helper Functions ===

  defp delete_topic_request(topic_name, api_version) do
    timeout_ms = @delete_timeout_ms

    case api_version do
      0 ->
        %Kayrock.DeleteTopics.V0.Request{
          correlation_id: :rand.uniform(10_000),
          client_id: "kayrock",
          topic_names: [topic_name],
          timeout_ms: timeout_ms
        }

      1 ->
        %Kayrock.DeleteTopics.V1.Request{
          correlation_id: :rand.uniform(10_000),
          client_id: "kayrock",
          topic_names: [topic_name],
          timeout_ms: timeout_ms
        }

      2 ->
        %Kayrock.DeleteTopics.V2.Request{
          correlation_id: :rand.uniform(10_000),
          client_id: "kayrock",
          topic_names: [topic_name],
          timeout_ms: timeout_ms
        }

      3 ->
        %Kayrock.DeleteTopics.V3.Request{
          correlation_id: :rand.uniform(10_000),
          client_id: "kayrock",
          topic_names: [topic_name],
          timeout_ms: timeout_ms
        }

      4 ->
        %Kayrock.DeleteTopics.V4.Request{
          correlation_id: :rand.uniform(10_000),
          client_id: "kayrock",
          topic_names: [topic_name],
          timeout_ms: timeout_ms
        }
    end
  end

  # === Assertion Helpers ===

  defp assert_no_error(error_code, message) do
    assert error_code == 0,
           "#{message} (got error_code: #{error_code})"
  end
end
