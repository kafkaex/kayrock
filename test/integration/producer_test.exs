defmodule Kayrock.Integration.ProducerTest do
  @moduledoc """
  Integration tests for Produce and Fetch APIs.

  Tests message production and consumption across different protocol versions
  and formats:
  - V0-V1: MessageSet format (legacy)
  - V2-V3: RecordBatch format (introduced in Kafka 0.11)
  - V4-V7: RecordBatch format with idempotence and transactions

  Each version is tested with single messages, multiple messages, and
  multi-partition/multi-topic scenarios.
  """
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "Produce API & Fetch API" do
    # V0-V1: MessageSet format
    for version <- [0, 1] do
      test "v#{version} - single message with MessageSet", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Produce single message
        messages = [build_message_set_message("test")]
        offset = produce_message_set(client_pid, topic, messages, version)

        # Fetch and verify
        fetched = fetch_message_set(client_pid, topic, offset, version)
        assert_message_values(fetched, ["test"], offset)
      end

      test "v#{version} - multiple messages with MessageSet", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Produce multiple messages
        messages = Enum.map(["foo", "bar", "baz"], &build_message_set_message/1)
        offset = produce_message_set(client_pid, topic, messages, version)

        # Fetch and verify
        fetched = fetch_message_set(client_pid, topic, offset, version)
        assert_message_values(fetched, ["foo", "bar", "baz"], offset)
      end
    end

    # V2-V3: RecordBatch format (basic)
    for version <- [2, 3] do
      test "v#{version} - single message with RecordBatch", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Produce single message
        messages = [build_record_batch_record("test")]
        offset = produce_record_batch(client_pid, topic, messages, version)

        # Fetch and verify
        fetched = fetch_record_batch(client_pid, topic, offset, version)
        assert_record_values(fetched, ["test"])
        assert_has_timestamp(fetched)
      end

      test "v#{version} - multiple messages with RecordBatch", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Produce multiple messages in one batch
        messages = Enum.map(["foo", "bar", "baz"], &build_record_batch_record/1)
        offset = produce_record_batch(client_pid, topic, messages, version)

        # Fetch and verify
        fetched = fetch_record_batch(client_pid, topic, offset, version)
        assert_record_values(fetched, ["foo", "bar", "baz"])

        # Produce another batch
        offset2 =
          produce_record_batch(client_pid, topic, [build_record_batch_record("zab")], version)

        # Fetch second batch
        fetched2 = fetch_record_batch(client_pid, topic, offset2, version)
        assert_record_values(fetched2, ["zab"])
        assert List.first(fetched2).offset == 3
      end
    end

    # V4-V7: RecordBatch format with headers
    for version <- [4, 5, 6, 7] do
      test "v#{version} - single message with headers", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Produce with headers
        messages = [build_record_with_headers("test")]
        offset = produce_record_batch(client_pid, topic, messages, version)

        # Fetch and verify
        fetched = fetch_record_batch_v4(client_pid, topic, offset, version)
        assert_record_values(fetched, ["test"])
        assert_has_headers(fetched)
      end

      test "v#{version} - multiple messages with headers and large content", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client_pid} = build_client(kafka)
        topic = create_topic(client_pid, version)

        # Generate test data
        long_header = random_string(12)
        content_prefix = random_string(50)

        # Produce multiple messages
        messages =
          Enum.map(1..3, fn i ->
            build_record_with_custom_headers(
              "#{content_prefix} #{i}",
              [%Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}]
            )
          end)

        offset = produce_record_batch(client_pid, topic, messages, version)

        # Fetch all messages
        fetched = fetch_record_batch_v4(client_pid, topic, offset, version)
        assert length(fetched) == 3

        # Verify content
        Enum.each(Enum.with_index(fetched, 1), fn {msg, i} ->
          assert msg.value == "#{content_prefix} #{i}"
          assert msg.offset == i - 1
          assert msg.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: long_header}]
        end)

        # Produce another message
        offset2 =
          produce_record_batch(client_pid, topic, [build_record_with_headers("zab")], version)

        fetched2 = fetch_record_batch_v4(client_pid, topic, offset2, version)

        assert List.first(fetched2).value == "zab"
        assert List.first(fetched2).offset == 3

        # Test partial fetch (max_bytes limit)
        partition_data = [[topic: topic, partition: 0, fetch_offset: 0]]
        fetch_request = fetch_messages_request(partition_data, [max_bytes: 100], version)
        {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

        [response] = resp.responses
        [%{records: records}] = List.first(response.partition_responses).record_set
        assert length(records) == 3
        assert List.first(records).value == "#{content_prefix} 1"

        # Test full fetch
        full_fetched = fetch_record_batch_v4(client_pid, topic, 0, version)
        assert length(full_fetched) == 4
      end
    end
  end

  describe "with non existing topic" do
    test "returns error code when producing to non-existent topic", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)
      non_existent_topic = unique_string()

      # Produce to non-existent topic
      messages = [build_record_with_headers("test")]
      record_set = %Kayrock.RecordBatch{records: messages}

      produce_request =
        produce_messages_request(non_existent_topic, [[record_set: record_set]], 1, 5)

      {:ok, response} = Kayrock.client_call(client_pid, produce_request, :controller)

      [topic_response] = response.responses
      assert topic_response.topic == non_existent_topic

      [partition_response] = topic_response.partition_responses
      assert partition_response.error_code == 3
    end
  end

  describe "with multiple topics and partitions" do
    test "produces to multiple partitions of single topic", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)
      topic = create_topic(client_pid, 5)

      # Produce to partition 0
      messages1 = [build_record_with_headers("test-one")]
      record_set1 = %Kayrock.RecordBatch{records: messages1}

      # Produce to partition 1
      messages2 = [build_record_with_headers("test-two")]
      record_set2 = %Kayrock.RecordBatch{records: messages2}

      # Send to both partitions
      produce_data = [
        [record_set: record_set1, partition: 0],
        [record_set: record_set2, partition: 1]
      ]

      produce_request = produce_messages_request(topic, produce_data, 1, 5)
      {:ok, response} = Kayrock.client_call(client_pid, produce_request, :controller)

      [topic_response] = response.responses
      [part0, part1] = topic_response.partition_responses |> Enum.sort_by(& &1.partition)

      assert part0.error_code == 0
      assert part1.error_code == 0

      # Fetch from both partitions
      partition_data = [
        [topic: topic, partition: 0, fetch_offset: part0.base_offset],
        [topic: topic, partition: 1, fetch_offset: part1.base_offset]
      ]

      fetch_request = fetch_messages_request(partition_data, [], 5)
      {:ok, fetch_resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

      [response] = fetch_resp.responses

      [batch0, batch1] =
        response.partition_responses |> Enum.sort_by(& &1.partition_header.partition)

      # Verify partition 0
      [msg0] = batch0.record_set |> List.first() |> Map.get(:records)
      assert msg0.value == "test-one"
      assert msg0.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]

      # Verify partition 1
      [msg1] = batch1.record_set |> List.first() |> Map.get(:records)
      assert msg1.value == "test-two"
      assert msg1.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
    end

    test "produces to multiple topics", %{kafka: kafka} do
      {:ok, client_pid} = build_client(kafka)
      topic1 = create_topic(client_pid, 5)
      topic2 = create_topic(client_pid, 5)

      # Produce to both topics
      produce_record_batch(client_pid, topic1, [build_record_with_headers("test-one")], 5)
      produce_record_batch(client_pid, topic2, [build_record_with_headers("test-two")], 5)

      # Fetch from both topics
      partition_data = [
        [topic: topic1, partition: 0, fetch_offset: 0],
        [topic: topic2, partition: 0, fetch_offset: 0]
      ]

      fetch_request = fetch_messages_request(partition_data, [], 5)
      {:ok, response} = Kayrock.client_call(client_pid, fetch_request, :controller)

      # Verify both topics
      response1 = Enum.find(response.responses, &(&1.topic == topic1))
      response2 = Enum.find(response.responses, &(&1.topic == topic2))

      [batch1] = response1.partition_responses
      [msg1] = batch1.record_set |> List.first() |> Map.get(:records)
      assert msg1.value == "test-one"

      [batch2] = response2.partition_responses
      [msg2] = batch2.record_set |> List.first() |> Map.get(:records)
      assert msg2.value == "test-two"
    end
  end

  # ============================================
  # Helper Functions
  # ============================================

  # Build MessageSet message (V0-V1)
  defp build_message_set_message(value) do
    %Kayrock.MessageSet.Message{
      key: "1",
      value: value,
      attributes: 0
    }
  end

  # Build RecordBatch record without headers (V2-V3)
  defp build_record_batch_record(value) do
    %Kayrock.RecordBatch.Record{
      key: "1",
      value: value,
      timestamp: DateTime.utc_now() |> DateTime.to_unix(:millisecond),
      attributes: 0
    }
  end

  # Build RecordBatch record with headers (V4+)
  defp build_record_with_headers(value) do
    %Kayrock.RecordBatch.Record{
      key: "1",
      value: value,
      timestamp: DateTime.utc_now() |> DateTime.to_unix(:millisecond),
      headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
      attributes: 0
    }
  end

  # Build RecordBatch record with custom headers
  defp build_record_with_custom_headers(value, headers) do
    %Kayrock.RecordBatch.Record{
      key: "1",
      value: value,
      timestamp: DateTime.utc_now() |> DateTime.to_unix(:millisecond),
      headers: headers,
      attributes: 0
    }
  end

  # Produce MessageSet and return base offset (V0-V1)
  defp produce_message_set(client_pid, topic, messages, api_version) do
    record_set = %Kayrock.MessageSet{messages: messages}

    produce_request =
      produce_messages_request(topic, [[record_set: record_set]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client_pid, produce_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses
    assert partition_response.error_code == 0

    partition_response.base_offset
  end

  # Produce RecordBatch and return base offset (V2+)
  defp produce_record_batch(client_pid, topic, records, api_version) do
    record_set = %Kayrock.RecordBatch{records: records}

    produce_request =
      produce_messages_request(topic, [[record_set: record_set]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client_pid, produce_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses
    assert partition_response.error_code == 0

    partition_response.base_offset
  end

  # Fetch MessageSet messages (V0-V1)
  defp fetch_message_set(client_pid, topic, offset, api_version) do
    partition_data = [[topic: topic, partition: 0, fetch_offset: offset]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client_pid, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    List.first(topic_response.partition_responses).record_set.messages
  end

  # Fetch RecordBatch messages (V2-V3)
  defp fetch_record_batch(client_pid, topic, offset, api_version) do
    partition_data = [[topic: topic, partition: 0, fetch_offset: offset, log_start_offset: 0]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client_pid, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    List.first(topic_response.partition_responses).record_set.messages
  end

  # Fetch RecordBatch messages (V4+, returns list of batches)
  defp fetch_record_batch_v4(client_pid, topic, offset, api_version) do
    partition_data = [[topic: topic, partition: 0, fetch_offset: offset]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client_pid, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    # V4+ returns list of batches, extract all records
    List.first(topic_response.partition_responses).record_set
    |> Enum.flat_map(fn batch -> Map.get(batch, :records) end)
  end

  # Assert message values match expected (MessageSet)
  defp assert_message_values(messages, expected_values, base_offset) do
    assert length(messages) == length(expected_values)

    messages
    |> Enum.zip(expected_values)
    |> Enum.with_index()
    |> Enum.each(fn {{message, expected_value}, index} ->
      assert message.value == expected_value
      assert message.offset == base_offset + index
    end)
  end

  # Assert record values match expected (RecordBatch)
  defp assert_record_values(records, expected_values) do
    assert length(records) == length(expected_values)

    records
    |> Enum.zip(expected_values)
    |> Enum.each(fn {record, expected_value} ->
      assert record.value == expected_value
    end)
  end

  # Assert records have timestamps
  defp assert_has_timestamp(records) do
    Enum.each(records, fn record ->
      assert is_integer(record.timestamp)
      assert record.timestamp > 0
    end)
  end

  # Assert records have headers
  defp assert_has_headers(records) do
    Enum.each(records, fn record ->
      assert is_list(record.headers)
      assert record.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
    end)
  end

  # Generate random string of given length
  defp random_string(length) do
    ?a..?z |> Enum.to_list() |> Enum.take_random(length) |> to_string()
  end
end
