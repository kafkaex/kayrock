defmodule Kayrock.Integration.ProducerTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.Convenience
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Produce API & Fetch API" do
    for version <- [0, 1] do
      test "v#{version} - produce and reads data using message set", %{kafka: kafka} do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)

        # Create Topic
        topic_name = create_topic(client_pid, api_version)

        # [GIVEN] MessageSet with timestamp
        record_set = %Kayrock.MessageSet{
          messages: [
            %Kayrock.MessageSet.Message{
              key: "1",
              value: "test",
              attributes: 0
            }
          ]
        }

        # [WHEN] Produce message with timestamp
        produce_message_request =
          produce_messages_request(topic_name, [record_set: record_set], 1, api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
        [response] = resp.responses
        assert response.topic == topic_name

        [partition_response] = response.partition_responses
        assert partition_response.error_code == 0
        offset = partition_response.base_offset

        # [THEN] Fetch message from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: offset]]
        fetch_request = fetch_messages_request(partition_data, [], api_version)
        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        assert response.topic == topic_name

        # [THEN] Verify message data
        [message] = List.first(response.partition_responses).record_set.messages
        assert message.value == "test"
      end
    end

    for version <- [2, 3] do
      test "v#{version} - produce and reads data using record batch", %{kafka: kafka} do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)

        # Create Topic
        topic_name = create_topic(client_pid, api_version)

        # [GIVEN] MessageSet with timestamp
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        record_set = %Kayrock.RecordBatch{
          records: [
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "test",
              headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
              timestamp: timestamp,
              attributes: 0
            }
          ]
        }

        # [WHEN] Produce message with timestamp
        produce_message_request =
          produce_messages_request(topic_name, [record_set: record_set], 1, api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
        [response] = resp.responses
        assert response.topic == topic_name

        [partition_response] = response.partition_responses
        assert partition_response.error_code == 0
        offset = partition_response.base_offset

        # [THEN] Fetch message from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: offset]]
        fetch_request = fetch_messages_request(partition_data, [], api_version)
        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        assert response.topic == topic_name

        # [THEN] Verify message data
        [message] = List.first(response.partition_responses).record_set.messages
        assert message.value == "test"
        assert message.timestamp == timestamp
      end
    end

    for version <- [4, 5, 6, 7] do
      test "v#{version} - produce and reads data using record batch", %{kafka: kafka} do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)

        # Create Topic
        topic_name = create_topic(client_pid, api_version)

        # [GIVEN] MessageSet with timestamp
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        record_set = %Kayrock.RecordBatch{
          records: [
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "test",
              headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
              timestamp: timestamp,
              attributes: 0
            }
          ]
        }

        # [WHEN] Produce message with timestamp
        produce_message_request =
          produce_messages_request(topic_name, [record_set: record_set], 1, api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
        [response] = resp.responses
        assert response.topic == topic_name

        [partition_response] = response.partition_responses
        assert partition_response.error_code == 0
        offset = partition_response.base_offset

        # [THEN] Fetch message from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: offset]]
        fetch_request = fetch_messages_request(partition_data, [], api_version)
        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        assert response.topic == topic_name

        # [THEN] Verify message data
        [message] =
          List.first(response.partition_responses).record_set |> List.first() |> Map.get(:records)

        assert message.value == "test"
        assert message.timestamp == timestamp
        assert message.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
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
