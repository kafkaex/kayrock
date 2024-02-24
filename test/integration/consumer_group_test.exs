defmodule Kayrock.Integration.ConsumerGroupTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Consumer Group API" do
    for api_version <- [0, 1, 2] do
      test "v#{api_version} - allows to manage consumer groups", %{kafka: kafka} do
        api_version = unquote(api_version)
        group_name = unique_string()

        {:ok, client_pid} = build_client(kafka)
        topic_name = create_topic(client_pid, api_version)

        # [WHEN] No consumer groups exist
        # [THEN] List consumer groups returns empty list
        list_request = list_consumer_groups_request(api_version)
        {:ok, list_response} = Kayrock.client_call(client_pid, list_request, :controller)

        matching_groups = Enum.filter(list_response.groups, &(&1.group_id == group_name))
        assert list_response.error_code == 0
        assert matching_groups == []

        # [WHEN] We try to find a coordinator for a consumer group
        coordinator_request = find_coordinator_request(group_name, api_version)

        {:ok, coordinator_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, coordinator_request, 1)
          end)

        # [THEN] We get a valid coordinator node
        assert coordinator_response.error_code == 0
        assert coordinator_response.coordinator.node_id > 0
        node_id = coordinator_response.coordinator.node_id

        # [WHEN] We join a group
        member_data = %{group_id: group_name, topics: [topic_name]}
        join_request = join_group_request(member_data, api_version)

        {:ok, join_response} =
          with_retry(fn ->
            Kayrock.client_call(client_pid, join_request, node_id)
          end)

        assert join_response.error_code == 0
        assert join_response.members != []
        member_ids = Enum.map(join_response.members, & &1.member_id)
        assert Enum.member?(member_ids, join_response.member_id)

        # [THEN] We can list the consumer group
        list_request = list_consumer_groups_request(api_version)
        {:ok, list_response} = Kayrock.client_call(client_pid, list_request, node_id)

        matching_groups = Enum.filter(list_response.groups, &(&1.group_id == group_name))
        assert list_response.error_code == 0
        assert matching_groups == [%{group_id: group_name, protocol_type: "consumer"}]

        # [WHEN] We sync the group
        assignments = [
          %{
            member_id: join_response.member_id,
            topic: topic_name,
            partitions: [0, 1, 2]
          }
        ]

        sync_request =
          sync_group_request(group_name, join_response.member_id, assignments, api_version)

        {:ok, sync_response} = Kayrock.client_call(client_pid, sync_request, node_id)
        assert sync_response.error_code == 0

        # [THEN] We can describe the consumer group
        describe_request = describe_groups_request([group_name], api_version)
        {:ok, describe_response} = Kayrock.client_call(client_pid, describe_request, node_id)

        [group_info] = describe_response.groups
        assert group_info.error_code == 0
        assert group_info.group_id == group_name
        assert group_info.protocol_type == "consumer"
        [member] = group_info.members
        assert member.member_id == join_response.member_id

        # [WHEN] We leave the group
        leave_group_request =
          leave_group_request(group_name, join_response.member_id, api_version)

        {:ok, leave_group_response} =
          Kayrock.client_call(client_pid, leave_group_request, node_id)

        assert leave_group_response.error_code == 0

        # [THEN] We can don't find member in the group
        describe_request = describe_groups_request([group_name], api_version)
        {:ok, describe_response} = Kayrock.client_call(client_pid, describe_request, node_id)

        [group_info] = describe_response.groups
        assert group_info.error_code == 0
        assert group_info.group_id == group_name
        assert group_info.protocol_type == "consumer"
        assert group_info.members == []

        # [WHEN] We delete consumer group
        delete_request = delete_groups_request([group_name], api_version)
        {:ok, delete_response} = Kayrock.client_call(client_pid, delete_request, node_id)

        assert delete_response.group_error_codes == [%{group_id: group_name, error_code: 0}]

        # [THEN] We can't find the group
        list_request = list_consumer_groups_request(api_version)
        {:ok, list_response} = Kayrock.client_call(client_pid, list_request, node_id)

        matching_groups = Enum.filter(list_response.groups, &(&1.group_id == group_name))
        assert matching_groups == []
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
