defmodule Kayrock.Integration.HeartbeatTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Heartbeat API" do
    for api_version <- [0, 1, 2, 3] do
      test "v#{api_version} - sends heartbeat for active group member", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic and group
        topic_name = create_topic(client_pid, api_version)
        group_id = "heartbeat-group-#{unique_string()}"

        # Find coordinator
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        assert coordinator_response.error_code == 0
        node_id = coordinator_response.node_id

        # Join group
        join_request =
          join_group_request(%{group_id: group_id, topics: [topic_name]}, min(api_version, 5))

        {:ok, join_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, join_request, node_id)
          end)

        assert join_response.error_code == 0
        generation_id = join_response.generation_id
        member_id = join_response.member_id

        # Sync group (required before heartbeat)
        assignments = [
          %{
            member_id: member_id,
            topic: topic_name,
            partitions: [0, 1, 2]
          }
        ]

        sync_request = sync_group_request(group_id, member_id, assignments, min(api_version, 3))
        {:ok, sync_response} = Kayrock.client_call(client_pid, sync_request, node_id)
        assert sync_response.error_code == 0

        # Send heartbeat
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)

        # Should succeed with no error
        assert heartbeat_response.error_code == 0
      end

      test "v#{api_version} - heartbeat with invalid member_id returns error", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create group (without joining)
        group_id = "heartbeat-invalid-#{unique_string()}"

        # Find coordinator
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

        # Should return UNKNOWN_MEMBER_ID (25) or similar error
        assert heartbeat_response.error_code != 0
      end

      test "v#{api_version} - heartbeat with wrong generation_id returns error", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic and group
        topic_name = create_topic(client_pid, api_version)
        group_id = "heartbeat-gen-#{unique_string()}"

        # Find coordinator
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        node_id = coordinator_response.node_id

        # Join group
        join_request =
          join_group_request(%{group_id: group_id, topics: [topic_name]}, min(api_version, 5))

        {:ok, join_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, join_request, node_id)
          end)

        assert join_response.error_code == 0
        member_id = join_response.member_id

        # Sync group
        assignments = [
          %{member_id: member_id, topic: topic_name, partitions: [0, 1, 2]}
        ]

        sync_request = sync_group_request(group_id, member_id, assignments, min(api_version, 3))
        {:ok, _} = Kayrock.client_call(client_pid, sync_request, node_id)

        # Send heartbeat with wrong generation_id
        wrong_generation = 999
        heartbeat_request = heartbeat_request(group_id, member_id, wrong_generation, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)

        # Should return ILLEGAL_GENERATION (22)
        assert heartbeat_response.error_code == 22
      end
    end

    test "multiple heartbeats keep session alive", %{kafka: kafka} do
      api_version = 2
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, api_version)
      group_id = "heartbeat-multi-#{unique_string()}"

      # Find coordinator
      coordinator_request = find_coordinator_request(group_id, api_version)

      {:ok, coordinator_response} =
        with_retry(fn ->
          Kayrock.client_call(client_pid, coordinator_request, 1)
        end)

      node_id = coordinator_response.node_id

      # Join group
      join_request = join_group_request(%{group_id: group_id, topics: [topic_name]}, api_version)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(client_pid, join_request, node_id)
        end)

      generation_id = join_response.generation_id
      member_id = join_response.member_id

      # Sync group
      assignments = [%{member_id: member_id, topic: topic_name, partitions: [0]}]
      sync_request = sync_group_request(group_id, member_id, assignments, api_version)
      {:ok, _} = Kayrock.client_call(client_pid, sync_request, node_id)

      # Send multiple heartbeats
      for _ <- 1..3 do
        heartbeat_request = heartbeat_request(group_id, member_id, generation_id, api_version)
        {:ok, heartbeat_response} = Kayrock.client_call(client_pid, heartbeat_request, node_id)
        assert heartbeat_response.error_code == 0
        :timer.sleep(100)
      end
    end
  end

  defp build_client(kafka) do
    uris = [{"localhost", Container.mapped_port(kafka, 9092)}]
    Kayrock.Client.start_link(uris)
  end

  defp create_topic(client_pid, api_version) do
    topic_name = unique_string()
    create_request = create_topic_request(topic_name, api_version)
    {:ok, _} = Kayrock.client_call(client_pid, create_request, :controller)
    topic_name
  end
end
