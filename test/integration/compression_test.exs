defmodule Kayrock.Client.CompressionTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Produce API & Fetch API with compression" do
    # MessageSet format (v0-v1) only supports gzip and snappy
    for {compression, _compression_num} <- [{"gzip", 1}, {"snappy", 2}] do
      for version <- [0, 1] do
        test "v#{version} - produce and reads data using message set with compression: #{compression}",
             %{kafka: kafka} do
          api_version = unquote(version)
          {:ok, client_pid} = build_client(kafka)

          # Create Topic
          topic_name = create_topic(client_pid, api_version)

          # [GIVEN] MessageSet with compression
          record_set = %Kayrock.MessageSet{
            messages: [
              %Kayrock.MessageSet.Message{
                compression: unquote(compression) |> String.to_atom(),
                key: "1",
                value: "foo",
                attributes: 0
              },
              %Kayrock.MessageSet.Message{
                compression: unquote(compression) |> String.to_atom(),
                key: "1",
                value: "bar",
                attributes: 0
              },
              %Kayrock.MessageSet.Message{
                compression: unquote(compression) |> String.to_atom(),
                key: "1",
                value: "baz",
                attributes: 0
              }
            ]
          }

          # [WHEN] Produce message
          produce_message_request =
            produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

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
          [message_one, message_two, message_three] =
            List.first(response.partition_responses).record_set.messages

          assert message_one.value == "foo"
          assert message_one.offset == 0

          assert message_two.value == "bar"
          assert message_two.offset == 1

          assert message_three.value == "baz"
          assert message_three.offset == 2
        end
      end
    end

    # RecordBatch format (v2-v6) supports all compression types including lz4
    for {compression, compression_num} <- [{"gzip", 1}, {"snappy", 2}, {"lz4", 3}] do
      for version <- [2, 3] do
        test "v#{version} - produce and reads data with compression: #{compression}", %{
          kafka: kafka
        } do
          compression_num = unquote(compression_num)
          api_version = unquote(version)
          {:ok, client_pid} = build_client(kafka)

          # Create Topic
          topic_name = create_topic(client_pid, api_version)
          timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

          # [GIVEN] RecordBatch with compression
          records = [
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "foo",
              timestamp: timestamp,
              attributes: 0
            },
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "bar",
              timestamp: timestamp,
              attributes: 0
            },
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "baz",
              timestamp: timestamp,
              attributes: 0
            }
          ]

          # [WHEN] Produce message with compression
          record_set = %Kayrock.RecordBatch{attributes: compression_num, records: records}

          produce_message_request =
            produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

          {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
          [response] = resp.responses
          assert response.topic == topic_name

          [partition_response] = response.partition_responses
          assert partition_response.error_code == 0

          # [THEN] Fetch message from topic
          partition_data = [
            [topic: topic_name, partition: 0, fetch_offset: 0, log_start_offset: 0]
          ]

          fetch_request = fetch_messages_request(partition_data, [], api_version)
          {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

          [response] = resp.responses
          assert response.topic == topic_name

          # [THEN] Verify message data
          [message_one, message_two, message_three] =
            List.first(response.partition_responses).record_set.messages

          assert message_one.value == "foo"
          assert message_one.offset == 0
          assert message_one.timestamp == timestamp

          assert message_two.value == "bar"
          assert message_two.offset == 1
          assert message_two.timestamp == timestamp

          assert message_three.value == "baz"
          assert message_three.offset == 2
          assert message_three.timestamp == timestamp

          # [THEN] Produce another message
          record = %Kayrock.RecordBatch.Record{
            key: "1",
            value: "zab",
            timestamp: timestamp,
            attributes: 0
          }

          record_set = %Kayrock.RecordBatch{records: [record]}

          # [WHEN] Produce message
          produce_message_request =
            produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

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
          assert message.value == "zab"
          assert message.offset == 3
          assert message.timestamp == timestamp
        end
      end

      for version <- [4, 5] do
        test "v#{version} - produce and reads data using message set with compression: #{compression}",
             %{kafka: kafka} do
          compression_num = unquote(compression_num)
          api_version = unquote(version)
          {:ok, client_pid} = build_client(kafka)

          # Create Topic
          topic_name = create_topic(client_pid, api_version)
          timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

          # [GIVEN] RecordBatch with compression
          record_set = %Kayrock.RecordBatch{
            attributes: compression_num,
            records: [
              %Kayrock.RecordBatch.Record{
                key: "1",
                value: "foo",
                timestamp: timestamp,
                headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
                attributes: 0
              },
              %Kayrock.RecordBatch.Record{
                key: "1",
                value: "bar",
                timestamp: timestamp,
                headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
                attributes: 0
              },
              %Kayrock.RecordBatch.Record{
                key: "1",
                value: "baz",
                timestamp: timestamp,
                headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
                attributes: 0
              }
            ]
          }

          # [WHEN] Produce message with compression
          produce_message_request =
            produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

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
          [message_one, message_two, message_three] =
            List.first(response.partition_responses).record_set
            |> List.first()
            |> Map.get(:records)

          assert message_one.value == "foo"
          assert message_one.offset == 0
          assert message_one.timestamp == timestamp
          assert message_one.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]

          assert message_two.value == "bar"
          assert message_two.offset == 1
          assert message_two.timestamp == timestamp
          assert message_two.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]

          assert message_three.value == "baz"
          assert message_three.offset == 2
          assert message_three.timestamp == timestamp

          assert message_three.headers == [
                   %Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}
                 ]

          # [THEN] Produce another message
          record = %Kayrock.RecordBatch.Record{
            key: "1",
            value: "zab",
            timestamp: timestamp,
            attributes: 0
          }

          record_set = %Kayrock.RecordBatch{records: [record]}

          # [WHEN] Produce message
          produce_message_request =
            produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

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
            List.first(response.partition_responses).record_set
            |> List.first()
            |> Map.get(:records)

          assert message.value == "zab"
          assert message.offset == 3
          assert message.timestamp == timestamp
        end
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
