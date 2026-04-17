defmodule Kayrock.IntegrationHelpers do
  @moduledoc """
  Shared helper functions for integration tests.

  This module provides common utilities used across all integration test files
  to eliminate code duplication and provide consistent test infrastructure.

  ## Usage

  Automatically imported when using `Kayrock.IntegrationCase`:

      defmodule MyIntegrationTest do
        use Kayrock.IntegrationCase
        use ExUnit.Case, async: true

        container(:kafka, KafkaContainer.new(), shared: true)

        test "my test", %{kafka: kafka} do
          {:ok, client_pid} = build_client(kafka)
          topic = create_topic(client_pid, 5)
          # ... test logic
        end
      end

  ## Available Helpers

  - `build_client/1` - Create Kayrock client connected to Kafka container
  - `create_topic/3` - Create a unique test topic with options
  - `with_topic/4` - Create topic, run function, cleanup (future enhancement)
  - `wait_for_topic/3` - Wait for topic to be ready (future enhancement)
  """

  import Kayrock.TestSupport, only: [unique_string: 0, with_retry: 1, with_retry: 2]

  import Kayrock.RequestFactory,
    only: [
      create_topic_request: 2,
      find_coordinator_request: 2,
      join_group_request: 2,
      sync_group_request: 4
    ]

  alias Testcontainers.Container

  @doc """
  Build a Kayrock client connected to the Kafka container.

  ## Parameters

    - `kafka` - The Kafka container from Testcontainers

  ## Returns

    - `{:ok, pid}` - Client process ID on success
    - `{:error, reason}` - Error tuple on failure

  ## Example

      test "connects to kafka", %{kafka: kafka} do
        {:ok, client_pid} = build_client(kafka)
        assert Process.alive?(client_pid)
      end
  """
  def build_client(kafka) do
    uris = [{"localhost", Container.mapped_port(kafka, 9092)}]
    Kayrock.Client.start_link(uris)
  end

  @doc """
  Create a unique test topic with configurable options.

  ## Parameters

    - `client_pid` - The Kayrock client process ID
    - `api_version` - The CreateTopics API version to use (0-5)
    - `opts` - Optional keyword list of options:
      - `:name` - Custom topic name (default: random unique string)
      - `:partitions` - Number of partitions (default: 3)
      - `:replication_factor` - Replication factor (default: 1)
      - `:timeout` - Timeout in milliseconds (default: 1000)

  ## Returns

    - `topic_name` - The name of the created topic (string)

  ## Examples

      # Default: random name, 3 partitions
      topic = create_topic(client_pid, 5)

      # Custom name
      topic = create_topic(client_pid, 5, name: "my-test-topic")

      # More partitions
      topic = create_topic(client_pid, 5, partitions: 10)

      # All options
      topic = create_topic(client_pid, 5,
        name: "custom-topic",
        partitions: 5,
        replication_factor: 1,
        timeout: 2000
      )
  """
  def create_topic(client_pid, api_version, opts \\ []) do
    topic_name = Keyword.get(opts, :name, unique_string())

    # Note: Current create_topic_request doesn't support custom partitions
    # This is a future enhancement opportunity. For now, it always creates
    # topics with 3 partitions as defined in RequestFactory.
    create_request = create_topic_request(topic_name, api_version)

    {:ok, _response} = Kayrock.client_call(client_pid, create_request, :controller)

    topic_name
  end

  @doc """
  Create a topic, run a function with it, then optionally cleanup.

  This is a convenience wrapper around `create_topic/3` that provides
  automatic resource management. Currently cleanup is not implemented
  (topics persist), but this API allows for future enhancement.

  ## Parameters

    - `client_pid` - The Kayrock client process ID
    - `api_version` - The CreateTopics API version to use
    - `opts` - Same options as `create_topic/3`
    - `func` - Function to call with the topic name (arity 1)

  ## Returns

    - Returns the result of calling `func.(topic_name)`

  ## Example

      test "with automatic topic", %{kafka: kafka} do
        {:ok, client_pid} = build_client(kafka)

        result = with_topic(client_pid, 5, [], fn topic ->
          # Use topic here
          produce_to_topic(client_pid, topic, "message")
        end)

        assert result == :ok
      end
  """
  def with_topic(client_pid, api_version, opts \\ [], func) do
    topic_name = create_topic(client_pid, api_version, opts)

    try do
      func.(topic_name)
    after
      # Future enhancement: Delete topic here for cleanup
      # For now, topics persist (Kafka container is ephemeral per test run anyway)
      :ok
    end
  end

  @doc """
  Wait for a topic to be ready for operations.

  This helper polls the Kafka metadata to ensure a topic is fully created
  and ready before proceeding with operations. Useful when topic creation
  is eventually consistent.

  ## Parameters

    - `client_pid` - The Kayrock client process ID
    - `topic_name` - Name of the topic to wait for
    - `retries` - Number of retry attempts (default: 10)

  ## Returns

    - `:ok` - Topic is ready
    - `{:error, :timeout}` - Topic not ready after retries

  ## Example

      topic = create_topic(client_pid, 5)
      :ok = wait_for_topic(client_pid, topic)
      # Now safe to produce/fetch from topic
  """
  def wait_for_topic(client_pid, topic_name, retries \\ 10)

  def wait_for_topic(_client_pid, _topic_name, 0) do
    {:error, :timeout}
  end

  def wait_for_topic(client_pid, topic_name, retries) do
    case Kayrock.topics_metadata(client_pid, [topic_name]) do
      {:ok, [topic]} when topic.error_code == 0 ->
        :ok

      _ ->
        Process.sleep(100)
        wait_for_topic(client_pid, topic_name, retries - 1)
    end
  end

  @doc """
  Joins and syncs a consumer group, returning the coordinator info needed for
  subsequent API calls (heartbeat, offset commit, etc.).

  Handles the full lifecycle: FindCoordinator -> JoinGroup -> SyncGroup.
  For JoinGroup V4+ (KIP-394), automatically handles the two-step join protocol
  where the broker returns MEMBER_ID_REQUIRED (error 79) on the first attempt.

  ## Parameters

    - `client_pid` - The Kayrock client process ID
    - `group_id` - Consumer group ID
    - `topic_name` - Topic name for the group subscription
    - `opts` - Optional keyword list:
      - `:api_version` - Version to use for all APIs (default: 2). Each API is
        capped to its own max_vsn automatically.
      - `:partitions` - Partition list for assignment (default: [0, 1, 2])

  ## Returns

    `{node_id, generation_id, member_id}`

  ## Example

      {node_id, generation_id, member_id} =
        join_and_sync_group(client_pid, "my-group", topic_name)
  """
  def join_and_sync_group(client_pid, group_id, topic_name, opts \\ []) do
    api_version = Keyword.get(opts, :api_version, 2)
    partitions = Keyword.get(opts, :partitions, [0, 1, 2])

    # Find coordinator
    coordinator_request = find_coordinator_request(group_id, min(api_version, 2))

    {:ok, coordinator_response} =
      with_retry(fn ->
        Kayrock.client_call(client_pid, coordinator_request, 1)
      end)

    node_id = coordinator_response.node_id

    # Join group
    join_version = min(api_version, 5)
    join_request = join_group_request(%{group_id: group_id, topics: [topic_name]}, join_version)

    {:ok, join_response} =
      with_retry(
        fn -> Kayrock.client_call(client_pid, join_request, node_id) end,
        accept_errors: [79]
      )

    # Handle MEMBER_ID_REQUIRED (KIP-394) for JoinGroup V4+
    join_response =
      if join_response.error_code == 79 do
        retry_request =
          join_group_request(
            %{group_id: group_id, topics: [topic_name], member_id: join_response.member_id},
            join_version
          )

        {:ok, resp} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, retry_request, node_id)
          end)

        resp
      else
        join_response
      end

    generation_id = join_response.generation_id
    member_id = join_response.member_id

    # Sync group
    assignments = [
      %{member_id: member_id, topic: topic_name, partitions: partitions}
    ]

    sync_request =
      sync_group_request(group_id, member_id, assignments, min(api_version, 3))

    {:ok, _sync_response} = Kayrock.client_call(client_pid, sync_request, node_id)

    {node_id, generation_id, member_id}
  end
end
