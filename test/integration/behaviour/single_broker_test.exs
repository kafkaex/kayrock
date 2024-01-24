defmodule Kayrock.Integration.Behaviour.SingleBrokerTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  test "for single broker lifecycle", %{kafka: kafka} do
    # [WHEN] There is client connected to broker
    {:ok, client_pid} = build_client(kafka)

    # [AND] There is a topic created
    topic_name = create_topic(client_pid, 0)

    # [WHEN] Client can read from empty topic
    fetch_request = fetch_messages_request([[topic: topic_name]], [], 5)
    {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

    # [THEN] It receives empty response
    [%{partition_responses: [%{record_set: record_set}]}] = resp.responses
    assert is_nil(record_set)

    # [WHEN] Client can write to topic
    record_set = record_set([{"1", "test-one"}])
    produce_request = produce_messages_request(topic_name, [[record_set: record_set]], 1, 5)
    {:ok, _resp} = Kayrock.client_call(client_pid, produce_request, :controller)

    # [AND] Fetch message from that topic
    fetch_request = fetch_messages_request([[topic: topic_name]], [], 5)
    {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

    # [THEN] It receives message
    [%{partition_responses: [%{record_set: [record_set]}]}] = resp.responses
    [record] = record_set.records
    assert record.key == "1"
    assert record.value == "test-one"
    assert record.offset == 0

    # [WHEN] Client can write multiple messages to topic
    record_set = record_set([{"2", "test-two"}, {"3", "test-three"}])
    produce_request = produce_messages_request(topic_name, [[record_set: record_set]], 1, 5)
    {:ok, _resp} = Kayrock.client_call(client_pid, produce_request, :controller)

    # [AND] Fetch messages from that topic
    fetch_request = fetch_messages_request([[topic: topic_name, fetch_offset: 1]], [], 5)
    {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

    # [THEN] It receives messages
    [%{partition_responses: [%{record_set: [record_set]}]}] = resp.responses
    [record_one, record_two] = record_set.records
    assert record_one.key == "2"
    assert record_one.value == "test-two"
    assert record_one.offset == 1

    assert record_two.key == "3"
    assert record_two.value == "test-three"
    assert record_two.offset == 2

    # [WHEN] Client is closed
    :ok = Kayrock.Client.stop(client_pid)

    # [AND] New client is created
    {:ok, client_pid} = build_client(kafka)

    # [AND] Fetch messages from that topic
    fetch_request = fetch_messages_request([[topic: topic_name, fetch_offset: 0]], [], 5)
    {:ok, resp} = Kayrock.client_call(client_pid, fetch_request, :controller)

    # [THEN] It receives messages
    [%{partition_responses: [%{record_set: [record_set_one, record_set_two]}]}] = resp.responses
    [record] = record_set_one.records
    assert record.key == "1"
    assert record.value == "test-one"
    assert record.offset == 0

    [record_one, record_two] = record_set_two.records
    assert record_one.key == "2"
    assert record_one.value == "test-two"
    assert record_one.offset == 1

    assert record_two.key == "3"
    assert record_two.value == "test-three"
    assert record_two.offset == 2
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

  defp record_set(key_values) do
    %Kayrock.RecordBatch{
      records:
        Enum.map(key_values, fn {key, value} ->
          %Kayrock.RecordBatch.Record{
            key: key,
            value: value
          }
        end)
    }
  end
end
