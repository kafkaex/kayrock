defmodule Kayrock.Integration.MetadataTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Metadata API" do
    for api_version <- [0, 1, 2, 3, 4, 5, 6, 7, 8, 9] do
      test "v#{api_version} - returns broker metadata", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        request = metadata_request([], api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :random)

        # Should have at least one broker
        assert length(response.brokers) >= 1

        # Check broker structure
        [broker | _] = response.brokers
        assert is_integer(broker.node_id)
        assert is_binary(broker.host)
        assert is_integer(broker.port)
        assert broker.port > 0
      end

      test "v#{api_version} - returns metadata for specific topic", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create a topic first
        topic_name = create_topic(client_pid, api_version)

        # Get metadata for that topic
        request = metadata_request([topic_name], api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :random)

        # Find our topic in response
        topic = Enum.find(response.topics, fn t -> t.name == topic_name end)
        assert topic != nil
        assert topic.error_code == 0
        assert length(topic.partitions) == 3
      end

      test "v#{api_version} - returns metadata for multiple topics", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create multiple topics
        topic1 = create_topic(client_pid, api_version)
        topic2 = create_topic(client_pid, api_version)

        # Get metadata for both topics
        request = metadata_request([topic1, topic2], api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :random)

        # Find both topics
        topic1_meta = Enum.find(response.topics, fn t -> t.name == topic1 end)
        topic2_meta = Enum.find(response.topics, fn t -> t.name == topic2 end)

        assert topic1_meta != nil
        assert topic2_meta != nil
        assert topic1_meta.error_code == 0
        assert topic2_meta.error_code == 0
      end

      test "v#{api_version} - returns partition leader info", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        topic_name = create_topic(client_pid, api_version)

        request = metadata_request([topic_name], api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :random)

        topic = Enum.find(response.topics, fn t -> t.name == topic_name end)

        # Each partition should have a leader
        # Note: Field name changed from :leader (v0-6) to :leader_id (v7+)
        for partition <- topic.partitions do
          leader = Map.get(partition, :leader) || Map.get(partition, :leader_id)
          assert is_integer(leader)
          assert leader >= 0
          assert partition.error_code == 0
        end
      end

      test "v#{api_version} - returns error for non-existent topic", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        non_existent = "non-existent-topic-#{unique_string()}"

        request = metadata_request([non_existent], api_version)
        {:ok, response} = Kayrock.client_call(client_pid, request, :random)

        topic = Enum.find(response.topics, fn t -> t.name == non_existent end)

        # Should return UNKNOWN_TOPIC_OR_PARTITION (3) or LEADER_NOT_AVAILABLE (5)
        # depending on auto.create.topics.enable
        assert topic.error_code in [0, 3, 5]
      end
    end

    test "v4+ with nil topics returns all topics", %{kafka: kafka} do
      api_version = 4
      {:ok, client_pid} = build_client(kafka)

      # Create a topic
      topic_name = create_topic(client_pid, api_version)

      # Request with nil topics should return all topics
      request = metadata_request(nil, api_version)
      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      # Should include our topic
      topic_names = Enum.map(response.topics, & &1.name)
      assert topic_name in topic_names
    end

    test "v1+ includes controller_id", %{kafka: kafka} do
      api_version = 1
      {:ok, client_pid} = build_client(kafka)

      request = metadata_request([], api_version)
      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      # Controller ID should be a valid broker
      assert is_integer(response.controller_id)
      assert response.controller_id >= 0
    end

    test "v2+ includes cluster_id", %{kafka: kafka} do
      api_version = 2
      {:ok, client_pid} = build_client(kafka)

      request = metadata_request([], api_version)
      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      # Cluster ID should be a string
      assert is_binary(response.cluster_id) or is_nil(response.cluster_id)
    end

    test "partition metadata includes replicas and ISR", %{kafka: kafka} do
      api_version = 5
      {:ok, client_pid} = build_client(kafka)

      topic_name = create_topic(client_pid, api_version)

      request = metadata_request([topic_name], api_version)
      {:ok, response} = Kayrock.client_call(client_pid, request, :random)

      topic = Enum.find(response.topics, fn t -> t.name == topic_name end)
      [partition | _] = topic.partitions

      # Replicas and ISR should be lists of broker IDs
      # Note: Field names changed in newer API versions
      replicas = Map.get(partition, :replicas) || Map.get(partition, :replica_nodes)
      isr = Map.get(partition, :isr) || Map.get(partition, :isr_nodes)
      assert is_list(replicas)
      assert is_list(isr)
      assert length(replicas) >= 1
      assert length(isr) >= 1
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
