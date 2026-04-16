defmodule Kayrock.Integration.ListOffsetsTest do
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  describe "ListOffsets API" do
    for api_version <- [0, 1, 2, 3, 4, 5] do
      test "v#{api_version} - gets earliest and latest offsets for empty topic", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic
        topic_name = create_topic(client_pid, api_version)

        # Get earliest offset (timestamp = -2)
        earliest_request =
          list_offsets_request(topic_name, [[partition: 0, timestamp: -2]], api_version)

        {:ok, earliest_resp} = Kayrock.client_call(client_pid, earliest_request, :controller)

        [topic_response] = earliest_resp.responses
        assert topic_response.topic == topic_name
        [partition_response] = topic_response.partition_responses
        assert partition_response.error_code == 0

        # For V0, offset is in a list; for V1+, it's a single value
        earliest_offset = get_offset(partition_response, api_version)
        assert earliest_offset == 0

        # Get latest offset (timestamp = -1)
        latest_request =
          list_offsets_request(topic_name, [[partition: 0, timestamp: -1]], api_version)

        {:ok, latest_resp} = Kayrock.client_call(client_pid, latest_request, :controller)

        [topic_response] = latest_resp.responses
        [partition_response] = topic_response.partition_responses
        assert partition_response.error_code == 0

        latest_offset = get_offset(partition_response, api_version)
        # Empty topic should have same earliest and latest
        assert latest_offset == 0
      end

      test "v#{api_version} - gets offsets after producing messages", %{kafka: kafka} do
        api_version = unquote(api_version)
        {:ok, client_pid} = build_client(kafka)

        # Create topic
        topic_name = create_topic(client_pid, api_version)

        # Produce some messages
        record_set = %Kayrock.RecordBatch{
          records: [
            %Kayrock.RecordBatch.Record{key: "1", value: "msg1"},
            %Kayrock.RecordBatch.Record{key: "2", value: "msg2"},
            %Kayrock.RecordBatch.Record{key: "3", value: "msg3"}
          ]
        }

        produce_request =
          produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

        {:ok, _} = Kayrock.client_call(client_pid, produce_request, :controller)

        # Get earliest offset
        earliest_request =
          list_offsets_request(topic_name, [[partition: 0, timestamp: -2]], api_version)

        {:ok, earliest_resp} = Kayrock.client_call(client_pid, earliest_request, :controller)

        [topic_response] = earliest_resp.responses
        [partition_response] = topic_response.partition_responses
        earliest_offset = get_offset(partition_response, api_version)
        assert earliest_offset == 0

        # Get latest offset
        latest_request =
          list_offsets_request(topic_name, [[partition: 0, timestamp: -1]], api_version)

        {:ok, latest_resp} = Kayrock.client_call(client_pid, latest_request, :controller)

        [topic_response] = latest_resp.responses
        [partition_response] = topic_response.partition_responses
        latest_offset = get_offset(partition_response, api_version)
        # Should be 3 (offset of next message to be written)
        assert latest_offset == 3
      end

    end
  end

  # V0 returns offsets as a list
  defp get_offset(%{offsets: [offset | _]}, 0), do: offset
  # V1+ returns offset as a single value
  defp get_offset(%{offset: offset}, _api_version), do: offset
end
