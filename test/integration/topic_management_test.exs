defmodule Kayrock.Integration.TopicManagementTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.TestSupport
  import Kayrock.Convenience
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "topic management API" do
    for version <- [0, 1, 2] do
      test "v#{version} - allows to manage topic", %{kafka: kafka} do
        uris = [{"localhost", Container.mapped_port(kafka, 9092)}]
        api_version = unquote(version)
        {:ok, client_pid} = Kayrock.Client.start_link(uris)
        topic_name = unique_string()

        # Get Topics
        refute topic_exists?(client_pid, topic_name)

        # Creates Topic
        create_request = create_topic_request(topic_name, api_version)
        {:ok, _} = Kayrock.client_call(client_pid, create_request, :controller)

        # Get Topic
        topic = get_topic_metadata(client_pid, topic_name)
        assert topic.name == topic_name
        assert length(topic.partitions) == 3

        # Create Partitions
        create_partition_config = create_topic_partition(topic_name, api_version)
        {:ok, res} = Kayrock.client_call(client_pid, create_partition_config, :controller)
        assert List.first(res.topic_errors).error_code == 0

        # Get Updated Topic
        topic = get_topic_metadata(client_pid, topic_name)
        assert length(topic.partitions) == 5

        # Update Topic Config
        alter_config = alter_topic_config(topic_name, api_version)
        {:ok, res} = Kayrock.client_call(client_pid, alter_config, :controller)
        assert List.first(res.resources).error_code == 0

        # Get Topic Config
        describe_config = describe_config(topic_name, api_version)
        {:ok, res} = Kayrock.client_call(client_pid, describe_config, :controller)
        resource = List.first(res.resources)
        assert resource.error_code == 0
        config = List.first(resource.config_entries)
        assert config.config_name == "cleanup.policy"
        assert config.config_value == "compact"

        # Deletes Topic
        max_version = min(Kayrock.DeleteTopics.max_vsn(), api_version)
        {:ok, _} = Kayrock.delete_topics(client_pid, [topic_name], 1000, max_version)

        # Get Topic
        refute topic_exists?(client_pid, topic_name)
      end
    end
  end

  defp create_topic_partition(topic_name, api_version) do
    api_version = min(Kayrock.CreatePartitions.max_vsn(), api_version)
    request = Kayrock.CreatePartitions.get_request_struct(api_version)
    partition_config = %{topic: topic_name, new_partitions: %{count: 5, assignment: nil}}
    %{request | topic_partitions: [partition_config], timeout: 1000, validate_only: false}
  end

  defp alter_topic_config(topic_name, api_version) do
    api_version = min(Kayrock.AlterConfigs.max_vsn(), api_version)
    request = Kayrock.AlterConfigs.get_request_struct(api_version)
    config = %{config_name: "cleanup.policy", config_value: "compact"}

    %{
      request
      | resources: [%{resource_type: 2, resource_name: topic_name, config_entries: [config]}],
        validate_only: false
    }
  end

  defp describe_config(topic_name, api_version) do
    api_version = min(Kayrock.DescribeConfigs.max_vsn(), api_version)
    request = Kayrock.DescribeConfigs.get_request_struct(api_version)

    %{
      request
      | resources: [
          %{resource_type: 2, resource_name: topic_name, config_names: ["cleanup.policy"]}
        ]
    }
  end

  def get_topic_metadata(pid, topic) when is_pid(pid) and is_binary(topic) do
    {:ok, [topic]} = Kayrock.topics_metadata(pid, [topic])
    topic
  end
end
