defmodule Kayrock.Chaos.TopicManagementTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @moderate_latency_ms 450
  @high_latency_ms 900
  @very_high_latency_ms 1_500
  @extreme_timeout_latency_ms 8_000

  @moderate_bandwidth_kbps 50
  @high_bandwidth_kbps 100

  @connection_drop_duration_ms 45
  @connection_recovery_wait_ms 150

  @packet_size_medium_bytes 100
  @packet_variation_medium_bytes 50
  @packet_delay_medium_us 1_000

  @topic_name_padding_bytes 50
  @multi_topic_count 2

  @delete_timeout_ms 9_000

  @timeout_toxic_ms 200

  @acceptable_create_errors [0, 36]

  describe "Topic creation under network latency" do
    @describetag chaos_type: :latency

    test "successfully creates topic with #{@high_latency_ms}ms high latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      topic_name = "chaos-create-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      [topic_result] = response.topics
      assert topic_result.name == topic_name
      assert topic_result.error_code in @acceptable_create_errors
    end

    test "creates #{@multi_topic_count} topics sequentially with #{@moderate_latency_ms}ms latency",
         ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      topics =
        for i <- 1..@multi_topic_count do
          "chaos-multi-#{i}-#{unique_string()}"
        end

      for topic_name <- topics do
        request = create_topic_request(topic_name, 5)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)

        [topic_result] = response.topics
        assert topic_result.name == topic_name
      end

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      metadata_request = metadata_request(topics, 9)

      {:ok, metadata_response} =
        Kayrock.client_call(ctx.client, metadata_request, :random)

      created_topics = Enum.map(metadata_response.topics, & &1.name)

      for topic_name <- topics do
        assert topic_name in created_topics
      end
    end
  end

  describe "Topic creation under connection failures" do
    @describetag chaos_type: :connection_failure

    test "recovers from brief connection drop during topic creation", ctx do
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      topic_name = "chaos-create-drop-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      [topic_result] = response.topics
      assert topic_result.name == topic_name
      assert topic_result.error_code in @acceptable_create_errors
    end
  end

  describe "Topic creation under bandwidth/packet constraints" do
    @tag chaos_type: :bandwidth
    test "creates topic with #{@moderate_bandwidth_kbps} KB/s bandwidth throttling", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @moderate_bandwidth_kbps)

      topic_name = "chaos-create-bw-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      [topic_result] = response.topics
      assert topic_result.name == topic_name
    end

    @tag chaos_type: :packet_manipulation
    test "handles #{@packet_size_medium_bytes}B packet fragmentation with long topic name", ctx do
      add_slicer(
        ctx.toxiproxy,
        ctx.proxy_name,
        @packet_size_medium_bytes,
        @packet_variation_medium_bytes,
        @packet_delay_medium_us
      )

      long_name =
        "chaos-fragmented-#{String.duplicate("x", @topic_name_padding_bytes)}-#{unique_string()}"

      request = create_topic_request(long_name, 5)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, request, :controller)
        end)

      [topic_result] = response.topics
      assert topic_result.name == long_name
    end
  end

  describe "Topic deletion under network chaos" do
    @tag chaos_type: :latency
    test "deletes topic with #{@high_latency_ms}ms high latency", ctx do
      topic_name = "chaos-delete-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @high_latency_ms)

      delete_request = delete_topic_request(topic_name, 4)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, delete_request, :controller)
        end)

      [topic_result] = response.responses
      assert topic_result.name == topic_name
    end

    @tag chaos_type: :connection_failure
    test "recovers from connection drop during topic deletion", ctx do
      topic_name = "chaos-delete-drop-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      delete_request = delete_topic_request(topic_name, 4)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, delete_request, :controller)
        end)

      [topic_result] = response.responses
      assert topic_result.name == topic_name
    end

    @tag chaos_type: :latency
    test "handles #{@very_high_latency_ms}ms slow deletion response", ctx do
      topic_name = "chaos-delete-slow-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @very_high_latency_ms)

      delete_request = delete_topic_request(topic_name, 4)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, delete_request, :controller)
        end)

      [topic_result] = response.responses
      assert topic_result.name == topic_name
    end
  end

  describe "Multiple topic operations under chaos" do
    @describetag chaos_type: :bulk_operations

    @tag chaos_type: :bandwidth
    test "deletes #{@multi_topic_count} topics with #{@high_bandwidth_kbps} KB/s bandwidth limits",
         ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @high_bandwidth_kbps)

      topics =
        for i <- 1..@multi_topic_count do
          name = "chaos-delete-multi-#{i}-#{unique_string()}"
          create_topic(ctx.client, 5, name: name)
          name
        end

      for topic_name <- topics do
        request = delete_topic_request(topic_name, 4)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, request, :controller)
          end)

        [topic_result] = response.responses
        assert topic_result.name == topic_name
      end

      assert length(topics) == @multi_topic_count
    end
  end

  describe "Negative scenarios - topic management failures" do
    @describetag chaos_type: :negative

    test "fails when #{@timeout_toxic_ms}ms timeout closes connection during create", ctx do
      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      topic_name = "chaos-create-timeout-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :controller)
      end)
    end

    test "fails when connection is permanently down during create", ctx do
      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)
      Process.sleep(50)

      topic_name = "chaos-create-down-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :controller)
      end)
    end

    test "fails when connection closes immediately (0ms timeout) during create", ctx do
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      topic_name = "chaos-create-immediate-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :controller)
      end)
    end

    test "fails when #{@extreme_timeout_latency_ms}ms latency exceeds client timeout during create",
         ctx do
      topic_name = "chaos-create-extreme-#{unique_string()}"
      request = create_topic_request(topic_name, 5)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, request, :controller)
      end)
    end

    test "fails when #{@timeout_toxic_ms}ms timeout closes connection during delete", ctx do
      topic_name = "chaos-delete-timeout-#{unique_string()}"
      create_request = create_topic_request(topic_name, 5)

      {:ok, _} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, create_request, :controller)
        end)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)

      delete_request = delete_topic_request(topic_name, 4)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, delete_request, :controller)
      end)
    end
  end

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
end
