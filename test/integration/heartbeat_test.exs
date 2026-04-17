defmodule Kayrock.Integration.HeartbeatTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Heartbeat API" do
    for api_version <- [0, 1, 2, 3] do
      test "v#{api_version} - sends heartbeat for active group member", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        topic_name = create_topic(client_pid, api_version)
        group_id = "heartbeat-group-#{unique_string()}"

        {node_id, generation_id, member_id} =
          join_and_sync_group(client_pid, group_id, topic_name, api_version: api_version)

        # Send heartbeat
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)

        assert heartbeat_response.error_code == 0
      end

      test "v#{api_version} - heartbeat with invalid member_id returns error", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        group_id = "heartbeat-invalid-#{unique_string()}"

        # Find coordinator without joining
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        node_id = coordinator_response.node_id

        # Send heartbeat with fake member_id
        heartbeat_request =
          heartbeat_request(group_id, "fake-member-id", 1, api_version)

        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)

        assert heartbeat_response.error_code != 0
      end

      test "v#{api_version} - heartbeat with wrong generation_id returns error", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        topic_name = create_topic(client_pid, api_version)
        group_id = "heartbeat-gen-#{unique_string()}"

        {node_id, _generation_id, member_id} =
          join_and_sync_group(client_pid, group_id, topic_name, api_version: api_version)

        # Send heartbeat with wrong generation_id
        heartbeat_request = heartbeat_request(group_id, member_id, 999, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)

        assert heartbeat_response.error_code == 22
      end
    end

    test "multiple heartbeats keep session alive", %{kafka: kafka} do
      api_version = 2
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, api_version)
      group_id = "heartbeat-multi-#{unique_string()}"

      {node_id, generation_id, member_id} =
        join_and_sync_group(client_pid, group_id, topic_name,
          api_version: api_version,
          partitions: [0]
        )

      # Send multiple heartbeats
      for _ <- 1..3 do
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)
        assert heartbeat_response.error_code == 0
        :timer.sleep(100)
      end
    end
  end
end
