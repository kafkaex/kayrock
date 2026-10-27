defmodule Kayrock.Kafka4.EndToEndTest do
  @moduledoc """
  End-to-end flows against `apache/kafka:4.4.0` using only the generated
  versions a 4.x broker still accepts (KIP-896): topic admin, metadata,
  produce/fetch, list offsets, the classic consumer-group protocol and
  offset management.

  This is the 4.x counterpart of the cp-kafka 7.4 `:integration` suite. Most
  of that suite cannot run unchanged against 4.4.0 because it creates topics
  with CreateTopics at the API version under test (v0/v1 are removed) and
  exercises removed versions (Fetch v0-v3, OffsetCommit v0-v1, ...); see
  `Kayrock.Kafka4.ApiVersionsSmokeTest` for the version table.
  """
  use Kayrock.Kafka4Case

  import Kayrock.RequestFactory
  import Kayrock.TestSupport, only: [unique_string: 0, with_retry: 1]

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  describe "topic management" do
    for version <- 2..Kayrock.CreateTopics.max_vsn() do
      test "CreateTopics v#{version} / DeleteTopics", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        name = unique_string()

        {:ok, %{topics: [created]}} =
          Kayrock.client_call(client, create_topic_request(name, version), :controller)

        assert created.name == name
        assert created.error_code == 0
        :ok = wait_for_leaders(client, name, 3)

        delete_version = min(version, Kayrock.DeleteTopics.max_vsn())
        request = Kayrock.DeleteTopics.get_request_struct(delete_version)
        request = %{request | topic_names: [name], timeout_ms: 10_000}

        {:ok, %{responses: [deleted]}} = Kayrock.client_call(client, request, :controller)
        assert deleted.name == name
        assert deleted.error_code == 0
      end
    end

    test "create_kafka4_topic/2 applies topic configs", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)

      topic =
        create_kafka4_topic(client, partitions: 1, configs: %{"cleanup.policy" => "compact"})

      request = %Kayrock.DescribeConfigs.V2.Request{
        resources: [
          %{resource_type: 2, resource_name: topic, config_names: ["cleanup.policy"]}
        ],
        include_synonyms: false
      }

      {:ok, %{resources: [resource]}} = Kayrock.client_call(client, request, :controller)
      assert resource.error_code == 0
      assert [%{config_name: "cleanup.policy", config_value: "compact"}] = resource.config_entries
    end
  end

  describe "metadata" do
    for version <- Kayrock.Metadata.min_vsn()..Kayrock.Metadata.max_vsn() do
      test "Metadata v#{version} describes a topic", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        topic = create_kafka4_topic(client, partitions: 2)

        request = metadata_request([topic], version)

        request =
          if version >= 8 do
            %{
              request
              | include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false
            }
          else
            request
          end

        request =
          if version >= 9,
            do: %{request | topics: [%{name: topic, tagged_fields: []}]},
            else: request

        {:ok, response} = Kayrock.client_call(client, request, :controller)
        assert [%{node_id: 1}] = response.brokers
        assert [%{error_code: 0, name: ^topic, partitions: partitions}] = response.topics
        assert length(partitions) == 2
      end
    end
  end

  describe "produce and fetch" do
    # Produce v0-v2 are advertised by 4.x but rejected (legacy message format).
    for version <- 3..Kayrock.Produce.max_vsn() do
      test "Produce v#{version} / Fetch round trip with headers", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        topic = create_kafka4_topic(client, partitions: 1)

        records =
          for i <- 1..3 do
            %Record{
              key: "k#{i}",
              value: "v#{i}",
              headers: [%RecordHeader{key: "h", value: "#{i}"}]
            }
          end

        assert produce(client, topic, records, version) == 0
        assert produce(client, topic, [%Record{value: "v4"}], version) == 3

        fetched = fetch(client, topic, 0, max(version, 4))
        assert Enum.map(fetched, & &1.value) == ["v1", "v2", "v3", "v4"]
        assert Enum.map(fetched, & &1.offset) == [0, 1, 2, 3]
        assert hd(fetched).headers == [%RecordHeader{key: "h", value: "1"}]
      end
    end

    # Fetch v0-v3 are removed in 4.x.
    for version <- 4..Kayrock.Fetch.max_vsn() do
      test "Fetch v#{version} from an offset", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        topic = create_kafka4_topic(client, partitions: 1)

        records = for i <- 1..5, do: %Record{value: "m#{i}"}
        assert produce(client, topic, records, Kayrock.Produce.max_vsn()) == 0

        values =
          client
          |> fetch(topic, 2, version)
          |> Enum.filter(&(&1.offset >= 2))
          |> Enum.map(& &1.value)

        assert values == ["m3", "m4", "m5"]
      end
    end

    test "producing to an unknown topic returns UNKNOWN_TOPIC_OR_PARTITION", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      record_set = %RecordBatch{records: [%Record{value: "x"}]}

      request =
        produce_messages_request(unique_string(), [[record_set: record_set]], 1, 8)

      {:ok, %{responses: [%{partition_responses: [partition]}]}} =
        Kayrock.client_call(client, request, :controller)

      assert partition.error_code == 3
    end
  end

  describe "list offsets" do
    # ListOffsets v0 is removed in 4.x.
    for version <- 1..Kayrock.ListOffsets.max_vsn() do
      test "ListOffsets v#{version} earliest / latest", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        topic = create_kafka4_topic(client, partitions: 1)
        records = [%Record{value: "a"}, %Record{value: "b"}]
        assert produce(client, topic, records, Kayrock.Produce.max_vsn()) == 0

        assert list_offset(client, topic, -2, version) == 0
        assert list_offset(client, topic, -1, version) == 2
      end
    end
  end

  describe "classic consumer group" do
    test "join, sync, heartbeat, commit, fetch, describe, leave, list, delete",
         %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      topic = create_kafka4_topic(client, partitions: 3)
      group_id = "kafka4-group-#{unique_string()}"

      {node_id, generation_id, member_id} =
        join_and_sync_group(client, group_id, topic, api_version: 5)

      for version <- 0..Kayrock.Heartbeat.max_vsn() do
        request = heartbeat_request(group_id, member_id, generation_id, version)
        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)
        assert response.error_code == 0, "Heartbeat v#{version}: #{inspect(response)}"
      end

      # OffsetCommit v0-v1 and OffsetFetch v0 are removed in 4.x.
      for version <- 2..Kayrock.OffsetCommit.max_vsn() do
        offset = 10 + version

        request =
          offset_commit_request(
            group_id,
            [[topic: topic, partitions: [[partition: 0, offset: offset, metadata: "m"]]]],
            version,
            generation_id: generation_id,
            member_id: member_id
          )

        {:ok, response} = Kayrock.client_call(client, request, node_id)

        assert [%{partitions: [%{error_code: 0}]}] = response.topics,
               "OffsetCommit v#{version}: #{inspect(response)}"

        for fetch_version <- 1..Kayrock.OffsetFetch.max_vsn() do
          request =
            offset_fetch_request(
              group_id,
              [[topic: topic, partitions: [[partition: 0]]]],
              fetch_version
            )

          {:ok, response} = Kayrock.client_call(client, request, node_id)
          assert [%{partitions: [partition]}] = response.topics
          assert partition.error_code == 0

          assert partition.committed_offset == offset,
                 "OffsetFetch v#{fetch_version} after OffsetCommit v#{version}"
        end
      end

      for version <- 0..Kayrock.DescribeGroups.max_vsn() do
        request = describe_groups_request([group_id], version)

        request =
          if version >= 3,
            do: %{request | include_authorized_operations: false},
            else: request

        {:ok, %{groups: [group]}} = Kayrock.client_call(client, request, node_id)
        assert group.error_code == 0
        assert group.group_id == group_id
        assert [%{member_id: ^member_id}] = group.members
      end

      {:ok, leave} =
        Kayrock.client_call(client, leave_group_request(group_id, member_id, 2), node_id)

      assert leave.error_code == 0

      for version <- 0..Kayrock.ListGroups.max_vsn() do
        {:ok, response} =
          Kayrock.client_call(client, list_consumer_groups_request(version), node_id)

        assert response.error_code == 0
      end

      {:ok, %{results: [deleted]}} =
        Kayrock.client_call(client, delete_groups_request([group_id], 1), node_id)

      assert deleted.group_id == group_id
      assert deleted.error_code == 0
    end
  end

  defp produce(client, topic, records, version) do
    record_set = %RecordBatch{records: records}
    request = produce_messages_request(topic, [[record_set: record_set]], 1, version)

    {:ok, %{responses: [%{partition_responses: [partition]}]}} =
      Kayrock.client_call(client, request, :controller)

    assert partition.error_code == 0, "Produce v#{version}: #{inspect(partition)}"
    partition.base_offset
  end

  defp fetch(client, topic, offset, version) do
    request =
      fetch_messages_request(
        [[topic: topic, fetch_offset: offset]],
        [max_wait_time: 100],
        version
      )

    # Fetch v9+ partitions also carry current_leader_epoch (-1 = unknown)
    request =
      if version >= 9,
        do: %{request | topics: Enum.map(request.topics, &with_leader_epoch/1)},
        else: request

    # Fetch v11 adds rack_id (non-nullable string)
    request = if version >= 11, do: %{request | rack_id: ""}, else: request

    {:ok, %{responses: [%{partition_responses: [partition]}]}} =
      Kayrock.client_call(client, request, :controller)

    assert partition.partition_header.error_code == 0
    Enum.flat_map(partition.record_set || [], & &1.records)
  end

  defp with_leader_epoch(topic) do
    %{topic | partitions: Enum.map(topic.partitions, &Map.put(&1, :current_leader_epoch, -1))}
  end

  defp list_offset(client, topic, timestamp, version) do
    request = list_offsets_request(topic, [[partition: 0, timestamp: timestamp]], version)

    {:ok, %{responses: [%{partition_responses: [partition]}]}} =
      Kayrock.client_call(client, request, :controller)

    assert partition.error_code == 0
    partition.offset
  end
end
