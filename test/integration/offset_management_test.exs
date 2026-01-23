defmodule Kayrock.Integration.OffsetManagementTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "OffsetCommit and OffsetFetch API" do
    for api_version <- [0, 1, 2, 3, 4, 5, 6, 7] do
      test "v#{api_version} - commit and fetch offsets for consumer group", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic and group
        topic_name = create_topic(client_pid, api_version)
        group_id = "test-group-#{unique_string()}"

        # Find coordinator for this group
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        assert coordinator_response.error_code == 0
        node_id = coordinator_response.node_id

        # Commit offset
        commit_request =
          offset_commit_request(
            group_id,
            [
              [
                topic: topic_name,
                partitions: [
                  [partition: 0, offset: 100, metadata: "test-metadata"]
                ]
              ]
            ],
            api_version
          )

        {:ok, commit_response} = Kayrock.client_call(client_pid, commit_request, node_id)

        [topic_result] = commit_response.topics
        assert topic_result.name == topic_name
        [partition_result] = topic_result.partitions
        assert partition_result.error_code == 0

        # Fetch offset
        fetch_request =
          offset_fetch_request(
            group_id,
            [[topic: topic_name, partitions: [[partition: 0]]]],
            api_version
          )

        {:ok, fetch_response} = Kayrock.client_call(client_pid, fetch_request, node_id)

        [topic_result] = fetch_response.responses
        assert topic_result.topic == topic_name
        [partition_result] = topic_result.partition_responses
        assert partition_result.error_code == 0
        assert partition_result.offset == 100

        # Metadata may or may not be returned depending on version
        if api_version >= 1 do
          assert partition_result.metadata == "test-metadata"
        end
      end

      test "v#{api_version} - commit offsets for multiple partitions", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic and group
        topic_name = create_topic(client_pid, api_version)
        group_id = "test-group-multi-#{unique_string()}"

        # Find coordinator
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        node_id = coordinator_response.node_id

        # Commit offsets for multiple partitions
        commit_request =
          offset_commit_request(
            group_id,
            [
              [
                topic: topic_name,
                partitions: [
                  [partition: 0, offset: 10],
                  [partition: 1, offset: 20],
                  [partition: 2, offset: 30]
                ]
              ]
            ],
            api_version
          )

        {:ok, commit_response} = Kayrock.client_call(client_pid, commit_request, node_id)

        [topic_result] = commit_response.topics
        partition_results = topic_result.partitions

        for partition_result <- partition_results do
          assert partition_result.error_code == 0
        end

        # Fetch all committed offsets
        fetch_request =
          offset_fetch_request(
            group_id,
            [
              [
                topic: topic_name,
                partitions: [
                  [partition: 0],
                  [partition: 1],
                  [partition: 2]
                ]
              ]
            ],
            api_version
          )

        {:ok, fetch_response} = Kayrock.client_call(client_pid, fetch_request, node_id)

        [topic_result] = fetch_response.responses
        partition_results = Enum.sort_by(topic_result.partition_responses, & &1.partition)

        [p0, p1, p2] = partition_results
        assert p0.offset == 10
        assert p1.offset == 20
        assert p2.offset == 30
      end

      test "v#{api_version} - fetch uncommitted offset returns -1", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic and group (without committing)
        topic_name = create_topic(client_pid, api_version)
        group_id = "test-group-empty-#{unique_string()}"

        # Find coordinator
        coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        node_id = coordinator_response.node_id

        # Fetch offset without prior commit
        fetch_request =
          offset_fetch_request(
            group_id,
            [[topic: topic_name, partitions: [[partition: 0]]]],
            api_version
          )

        {:ok, fetch_response} = Kayrock.client_call(client_pid, fetch_request, node_id)

        [topic_result] = fetch_response.responses
        [partition_result] = topic_result.partition_responses

        # -1 indicates no committed offset
        assert partition_result.offset == -1
      end
    end

    test "offset commit with generation_id and member_id (V1+)", %{kafka: kafka} do
      api_version = 2
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, api_version)
      group_id = "test-group-gen-#{unique_string()}"

      # Find coordinator
      coordinator_request = find_coordinator_request(group_id, api_version)

      {:ok, coordinator_response} =
        with_retry(fn ->
          Kayrock.client_call(client_pid, coordinator_request, 1)
        end)

      node_id = coordinator_response.node_id

      # Join group to get generation_id and member_id
      join_request =
        join_group_request(%{group_id: group_id, topics: [topic_name]}, api_version)

      {:ok, join_response} =
        with_retry(fn ->
          Kayrock.client_call(client_pid, join_request, node_id)
        end)

      assert join_response.error_code == 0
      generation_id = join_response.generation_id
      member_id = join_response.member_id

      # Commit offset with generation and member info
      commit_request =
        offset_commit_request(
          group_id,
          [[topic: topic_name, partitions: [[partition: 0, offset: 50]]]],
          api_version,
          generation_id: generation_id,
          member_id: member_id
        )

      {:ok, commit_response} = Kayrock.client_call(client_pid, commit_request, node_id)

      [topic_result] = commit_response.topics
      [partition_result] = topic_result.partitions
      assert partition_result.error_code == 0

      # Verify offset was committed
      fetch_request =
        offset_fetch_request(
          group_id,
          [[topic: topic_name, partitions: [[partition: 0]]]],
          api_version
        )

      {:ok, fetch_response} = Kayrock.client_call(client_pid, fetch_request, node_id)

      [topic_result] = fetch_response.responses
      [partition_result] = topic_result.partition_responses
      assert partition_result.offset == 50
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
