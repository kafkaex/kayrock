defmodule Kayrock.Chaos.HeartbeatTest do
  use Kayrock.ChaosCase
  use ExUnit.Case, async: false

  @moderate_latency_ms 100
  @extreme_timeout_latency_ms 8_000

  @low_bandwidth_kbps 50

  @connection_drop_duration_ms 30
  @connection_recovery_wait_ms 50
  @member_eviction_wait_ms 500

  @heartbeat_interval_ms 100
  @combined_phase_wait_ms 50

  @heartbeat_count 2
  @timeout_toxic_ms 5

  describe "Heartbeat with latency - success scenarios" do
    @describetag chaos_type: :latency

    test "succeeds with #{@moderate_latency_ms}ms moderate latency", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert response.error_code == 0
    end

    test "sends #{@heartbeat_count} heartbeats successfully", ctx do
      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)

      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      for _ <- 1..@heartbeat_count do
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

        {:ok, response} =
          with_retry(fn ->
            Kayrock.client_call(ctx.client, heartbeat_request, node_id)
          end)

        assert response.error_code == 0
        Process.sleep(@heartbeat_interval_ms)
      end
    end
  end

  describe "Heartbeat with latency - failure scenarios" do
    @describetag chaos_type: :latency_failure

    test "times out with #{@extreme_timeout_latency_ms}ms extreme latency", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @extreme_timeout_latency_ms)
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, node_id)
      end)
    end
  end

  describe "Heartbeat with connection failures - success scenarios" do
    @describetag chaos_type: :connection_failure

    test "recovers after brief connection drop", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert response.error_code == 0
    end

    test "returns eviction error after #{@member_eviction_wait_ms}ms prolonged failure", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@member_eviction_wait_ms)

      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@connection_recovery_wait_ms)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert response.error_code in [25, 27],
             "Member should be evicted after #{@member_eviction_wait_ms}ms downtime (got: #{response.error_code})"
    end
  end

  describe "Heartbeat with connection failures - failure scenarios" do
    @describetag chaos_type: :connection_failure

    test "fails when connection is down", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_down(ctx.toxiproxy, ctx.proxy_name)
      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)

      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, node_id)
      end)
    end
  end

  describe "Heartbeat with timeout toxic - failure scenarios" do
    @describetag chaos_type: :timeout

    test "fails when #{@timeout_toxic_ms}ms timeout toxic closes connection", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, @timeout_toxic_ms)
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, node_id)
      end)
    end

    test "fails when connection closes immediately (0ms timeout)", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_timeout(ctx.toxiproxy, ctx.proxy_name, 0)
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      assert_client_fails(fn ->
        Kayrock.client_call(ctx.client, heartbeat_request, node_id)
      end)
    end
  end

  describe "Heartbeat with bandwidth constraints" do
    @describetag chaos_type: :bandwidth

    test "succeeds with #{@low_bandwidth_kbps} KB/s bandwidth throttling", ctx do
      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @low_bandwidth_kbps)

      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)
      heartbeat_request = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat_request, node_id)
        end)

      assert response.error_code == 0
    end
  end

  describe "Combined heartbeat chaos scenarios" do
    @describetag chaos_type: :combined

    test "survives three sequential chaos phases", ctx do
      {group_id, member_id, generation_id, node_id} = setup_active_member(ctx)

      add_latency(ctx.toxiproxy, ctx.proxy_name, @moderate_latency_ms)
      heartbeat1 = heartbeat_request(group_id, member_id, generation_id, 3)
      {:ok, response1} = Kayrock.client_call(ctx.client, heartbeat1, node_id)
      assert response1.error_code == 0

      remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)
      add_down(ctx.toxiproxy, ctx.proxy_name)
      Process.sleep(@connection_drop_duration_ms)
      remove_toxic(ctx.toxiproxy, ctx.proxy_name, "down_downstream")
      Process.sleep(@combined_phase_wait_ms)

      heartbeat2 = heartbeat_request(group_id, member_id, generation_id, 3)

      {:ok, response2} =
        with_retry(fn ->
          Kayrock.client_call(ctx.client, heartbeat2, node_id)
        end)

      assert response2.error_code == 0

      add_bandwidth_limit(ctx.toxiproxy, ctx.proxy_name, @low_bandwidth_kbps)
      heartbeat3 = heartbeat_request(group_id, member_id, generation_id, 3)
      {:ok, response3} = Kayrock.client_call(ctx.client, heartbeat3, node_id)
      assert response3.error_code == 0
    end
  end
end
