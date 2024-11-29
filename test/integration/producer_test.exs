defmodule Kayrock.Integration.ProducerTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
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
        assert message.value == "test"
        assert message.offset == offset
      end

      test "v#{version} - produce and reads data using message set with multiple messages", %{
        kafka: kafka
      } do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)

        # Create Topic
        topic_name = create_topic(client_pid, api_version)

        # [GIVEN] MessageSet with timestamp
        record_set = %Kayrock.MessageSet{
          messages: [
            %Kayrock.MessageSet.Message{
              key: "1",
              value: "foo",
              attributes: 0
            },
            %Kayrock.MessageSet.Message{
              key: "1",
              value: "bar",
              attributes: 0
            },
            %Kayrock.MessageSet.Message{
              key: "1",
              value: "baz",
              attributes: 0
            }
          ]
        }

        # [WHEN] Produce message with timestamp
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
        assert message_one.offset == offset

        assert message_two.value == "bar"
        assert message_two.offset == offset + 1

        assert message_three.value == "baz"
        assert message_three.offset == offset + 2
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
        assert message.value == "test"
        assert message.timestamp == timestamp
      end

      test "v#{version} - produce and reads data using message set with multiple messages", %{
        kafka: kafka
      } do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)

        # Create Topic
        topic_name = create_topic(client_pid, api_version)
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        # [GIVEN] MessageSet with timestamp
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

        # [WHEN] Produce message with timestamp
        record_set = %Kayrock.RecordBatch{records: records}

        produce_message_request =
          produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
        [response] = resp.responses
        assert response.topic == topic_name

        [partition_response] = response.partition_responses
        assert partition_response.error_code == 0

        # [THEN] Fetch message from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: 0, log_start_offset: 0]]
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

        # [WHEN] Produce message with timestamp
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
          List.first(response.partition_responses).record_set |> List.first() |> Map.get(:records)

        assert message.value == "test"
        assert message.timestamp == timestamp
        assert message.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
      end

      test "v#{version} - produce and reads data using message set with multiple messages", %{
        kafka: kafka
      } do
        api_version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        long_header = ?a..?z |> Enum.to_list() |> Enum.take_random(12) |> to_string()
        message_content = ?a..?z |> Enum.to_list() |> Enum.take_random(50) |> to_string()

        # Create Topic
        topic_name = create_topic(client_pid, api_version)
        timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

        # [GIVEN] MessageSet with timestamp
        record_set = %Kayrock.RecordBatch{
          records: [
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "#{message_content} 1",
              timestamp: timestamp,
              headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}],
              attributes: 0
            },
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "#{message_content} 2",
              timestamp: timestamp,
              headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}],
              attributes: 0
            },
            %Kayrock.RecordBatch.Record{
              key: "1",
              value: "#{message_content} 3",
              timestamp: timestamp,
              headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}],
              attributes: 0
            }
          ]
        }

        # [WHEN] Produce message with timestamp
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
          List.first(response.partition_responses).record_set |> List.first() |> Map.get(:records)

        assert message_one.value == "#{message_content} 1"
        assert message_one.offset == 0
        assert message_one.timestamp == timestamp

        assert message_one.headers == [
                 %Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}
               ]

        assert message_two.value == "#{message_content} 2"
        assert message_two.offset == 1
        assert message_two.timestamp == timestamp

        assert message_two.headers == [
                 %Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}
               ]

        assert message_three.value == "#{message_content} 3"
        assert message_three.offset == 2
        assert message_three.timestamp == timestamp

        assert message_three.headers == [
                 %Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}
               ]

        # [THEN] Produce another message
        record = %Kayrock.RecordBatch.Record{
          key: "1",
          value: "zab",
          timestamp: timestamp,
          attributes: 0
        }

        record_set = %Kayrock.RecordBatch{records: [record]}

        # [WHEN] Produce message with timestamp
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
          List.first(response.partition_responses).record_set |> List.first() |> Map.get(:records)

        assert message.value == "zab"
        assert message.offset == 3
        assert message.timestamp == timestamp

        # [THEN] Fetch incomplete messages from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: 0]]
        fetch_request = fetch_messages_request(partition_data, [max_bytes: 100], api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        assert response.topic == topic_name

        # [THEN] Verify message data
        [%{records: records}] = List.first(response.partition_responses).record_set
        assert length(records) == 3

        assert List.first(records).value == "#{message_content} 1"
        assert List.first(records).offset == 0

        assert List.first(records).headers == [
                 %Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}
               ]

        assert List.last(records).value == "#{message_content} 3"
        assert List.last(records).offset == 2

        assert List.last(records).headers == [
                 %Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}
               ]

        # [THEN] Fetch complete messages from topic
        partition_data = [[topic: topic_name, partition: 0, fetch_offset: 0]]
        fetch_request = fetch_messages_request(partition_data, [], api_version)

        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        assert response.topic == topic_name

        # [THEN] Verify message data
        [%{records: records}, %{records: records_two}] =
          List.first(response.partition_responses).record_set

        assert length(records) == 3

        assert List.first(records).value == "#{message_content} 1"
        assert List.first(records).offset == 0

        assert List.last(records).value == "#{message_content} 3"
        assert List.last(records).offset == 2

        assert length(records_two) == 1
        assert List.first(records_two).value == "zab"
        assert List.first(records_two).offset == 3
      end
    end
  end

  describe "with non existing topic" do
    test "it will return error code", %{kafka: kafka} do
      api_version = 5
      {:ok, client_pid} = build_client(kafka)

      # Create Topic
      topic_name = unique_string()

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
        produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

      {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
      [response] = resp.responses
      assert response.topic == topic_name

      [partition_response] = response.partition_responses
      assert partition_response.error_code == 3
    end
  end

  describe "with multiple topics and partitions" do
    test "with multiple partitions for single topic", %{kafka: kafka} do
      api_version = 5
      {:ok, client_pid} = build_client(kafka)

      # Create Topic
      topic_name = create_topic(client_pid, api_version)

      # [GIVEN] MessageSet with timestamp
      timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      record_set_one = %Kayrock.RecordBatch{
        records: [
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: "test-one",
            headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
            timestamp: timestamp,
            attributes: 0
          }
        ]
      }

      record_set_two = %Kayrock.RecordBatch{
        records: [
          %Kayrock.RecordBatch.Record{
            key: "2",
            value: "test-two",
            headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
            timestamp: timestamp,
            attributes: 0
          }
        ]
      }

      # [WHEN] Produce message with timestamp
      produce_data = [
        [record_set: record_set_one, partition: 0],
        [record_set: record_set_two, partition: 1]
      ]

      produce_message_request =
        produce_messages_request(topic_name, produce_data, 1, api_version)

      {:ok, resp} = Kayrock.client_call(client_pid, produce_message_request, :controller)
      [response] = resp.responses
      assert response.topic == topic_name

      [partition_one_resp, partition_two_resp] =
        response.partition_responses |> Enum.sort_by(& &1.partition)

      assert partition_one_resp.error_code == 0
      assert partition_two_resp.error_code == 0

      partition_one_offset = partition_one_resp.base_offset
      partition_two_offset = partition_two_resp.base_offset

      # [THEN] Fetch message from topic
      partition_data = [
        [topic: topic_name, partition: 0, fetch_offset: partition_one_offset],
        [topic: topic_name, partition: 1, fetch_offset: partition_two_offset]
      ]

      fetch_request = fetch_messages_request(partition_data, [], api_version)
      {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

      [response] = resp.responses
      assert response.topic == topic_name

      # [THEN] Verify message data
      [record_batch_one, record_batch_two] =
        Enum.sort_by(response.partition_responses, & &1.partition_header.partition)

      assert record_batch_one.partition_header.partition == 0
      assert record_batch_two.partition_header.partition == 1

      [message_one] = record_batch_one.record_set |> List.first() |> Map.get(:records)
      assert message_one.value == "test-one"
      assert message_one.timestamp == timestamp
      assert message_one.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]

      [message_two] = record_batch_two.record_set |> List.first() |> Map.get(:records)
      assert message_two.value == "test-two"
      assert message_two.timestamp == timestamp
      assert message_two.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
    end

    test "with multiple topics for single partition", %{kafka: kafka} do
      api_version = 5
      {:ok, client_pid} = build_client(kafka)

      # Create Topic
      topic_name_one = create_topic(client_pid, api_version)
      topic_name_two = create_topic(client_pid, api_version)

      # [GIVEN] MessageSet with timestamp
      timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

      record_set_one = %Kayrock.RecordBatch{
        records: [
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: "test-one",
            headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
            timestamp: timestamp,
            attributes: 0
          }
        ]
      }

      record_set_two = %Kayrock.RecordBatch{
        records: [
          %Kayrock.RecordBatch.Record{
            key: "2",
            value: "test-two",
            headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
            timestamp: timestamp,
            attributes: 0
          }
        ]
      }

      # [WHEN] Produce message with timestamp
      produce_request_one =
        produce_messages_request(topic_name_one, [[record_set: record_set_one]], 1, api_version)

      produce_request_two =
        produce_messages_request(topic_name_two, [[record_set: record_set_two]], 1, api_version)

      {:ok, _resp} = Kayrock.client_call(client_pid, produce_request_one, :controller)
      {:ok, _resp} = Kayrock.client_call(client_pid, produce_request_two, :controller)

      # [THEN] Fetch message from topic
      partition_data = [
        [topic: topic_name_one, partition: 0, fetch_offset: 0],
        [topic: topic_name_two, partition: 0, fetch_offset: 0]
      ]

      fetch_request = fetch_messages_request(partition_data, [], api_version)
      {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

      response_one = Enum.find(resp.responses, &(&1.topic == topic_name_one))
      response_two = Enum.find(resp.responses, &(&1.topic == topic_name_two))

      # [THEN] Verify message data
      [record_batch_one] = response_one.partition_responses
      assert record_batch_one.partition_header.partition == 0

      [message_one] = record_batch_one.record_set |> List.first() |> Map.get(:records)
      assert message_one.value == "test-one"
      assert message_one.timestamp == timestamp
      assert message_one.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]

      [record_batch_two] = response_two.partition_responses
      assert record_batch_two.partition_header.partition == 0

      [message_two] = record_batch_two.record_set |> List.first() |> Map.get(:records)
      assert message_two.value == "test-two"
      assert message_two.timestamp == timestamp
      assert message_two.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
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
