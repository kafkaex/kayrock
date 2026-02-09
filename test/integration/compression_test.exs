defmodule Kayrock.Client.CompressionTest do
  @moduledoc """
  Integration tests for Produce/Fetch APIs with compression.

  Tests various compression formats across different API versions:
  - V0-V1: MessageSet format (gzip, snappy)
  - V2-V3: RecordBatch format (gzip, snappy, lz4)
  - V4-V5: RecordBatch format with headers (gzip, snappy, lz4)
  - V7-V8: RecordBatch format with zstd support (gzip, snappy, lz4, zstd)
  """
  use Kayrock.IntegrationCase
  use ExUnit.Case, async: false

  import Kayrock.RequestFactory

  container(:kafka, KafkaContainer.new(), shared: true)

  # Compression test matrix: {versions, format, compressions}
  @compression_matrix [
    {[0, 1], :message_set, [{"gzip", 1}, {"snappy", 2}]},
    {[2, 3], :record_batch, [{"gzip", 1}, {"snappy", 2}, {"lz4", 3}]},
    {[4, 5], :record_batch_headers, [{"gzip", 1}, {"snappy", 2}, {"lz4", 3}]},
    {[7, 8], :record_batch_headers, [{"gzip", 1}, {"snappy", 2}, {"lz4", 3}, {"zstd", 4}]}
  ]

  describe "Produce API & Fetch API with compression" do
    # Generate tests for all version/compression combinations
    for {versions, format, compressions} <- @compression_matrix do
      for version <- versions do
        for {compression_name, compression_num} <- compressions do
          test "v#{version} #{format} with #{compression_name} compression", %{kafka: kafka} do
            version = unquote(version)
            format = unquote(format)
            compression_name = unquote(compression_name)
            compression_num = unquote(compression_num)

            test_compression_roundtrip(
              kafka,
              version,
              format,
              compression_name,
              compression_num
            )
          end
        end
      end
    end
  end

  # Test helper: compression roundtrip for all formats
  defp test_compression_roundtrip(kafka, api_version, format, compression_name, compression_num) do
    {:ok, client_pid} = build_client(kafka)
    topic_name = create_topic(client_pid, api_version)

    # Create messages with compression
    test_values = ["foo", "bar", "baz"]

    record_set =
      build_compressed_record_set(format, compression_name, compression_num, test_values)

    offset = produce_and_verify(client_pid, topic_name, record_set, api_version)
    fetched_messages = fetch_and_extract(client_pid, topic_name, offset, api_version, format)

    assert_messages_correct(fetched_messages, test_values, format)
  end

  # Build compressed record set based on format
  defp build_compressed_record_set(:message_set, compression_name, _compression_num, values) do
    %Kayrock.MessageSet{
      messages:
        Enum.map(values, fn value ->
          %Kayrock.MessageSet.Message{
            compression: String.to_atom(compression_name),
            key: "1",
            value: value,
            attributes: 0
          }
        end)
    }
  end

  defp build_compressed_record_set(:record_batch, _compression_name, compression_num, values) do
    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    %Kayrock.RecordBatch{
      attributes: compression_num,
      records:
        Enum.map(values, fn value ->
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: value,
            timestamp: timestamp,
            attributes: 0
          }
        end)
    }
  end

  defp build_compressed_record_set(
         :record_batch_headers,
         _compression_name,
         compression_num,
         values
       ) do
    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)

    %Kayrock.RecordBatch{
      attributes: compression_num,
      records:
        Enum.map(values, fn value ->
          %Kayrock.RecordBatch.Record{
            key: "1",
            value: value,
            timestamp: timestamp,
            headers: [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}],
            attributes: 0
          }
        end)
    }
  end

  # Produce messages and verify success
  defp produce_and_verify(client_pid, topic_name, record_set, api_version) do
    produce_request =
      produce_messages_request(topic_name, [[record_set: record_set]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client_pid, produce_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic_name

    [partition_response] = topic_response.partition_responses
    assert partition_response.error_code == 0

    partition_response.base_offset
  end

  # Fetch messages and extract from response
  defp fetch_and_extract(client_pid, topic_name, offset, api_version, format) do
    partition_data = [[topic: topic_name, partition: 0, fetch_offset: offset]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client_pid, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic_name

    [partition_response] = topic_response.partition_responses

    # Extract messages based on format
    case format do
      :message_set ->
        partition_response.record_set.messages

      :record_batch ->
        partition_response.record_set.messages

      :record_batch_headers ->
        # V4+ returns list of batches
        partition_response.record_set
        |> List.first()
        |> Map.get(:records)
    end
  end

  # Verify message values and offsets
  defp assert_messages_correct(messages, expected_values, format) do
    assert length(messages) == length(expected_values)

    messages
    |> Enum.zip(expected_values)
    |> Enum.with_index()
    |> Enum.each(fn {{message, expected_value}, index} ->
      assert message.value == expected_value
      assert message.offset == index

      # Verify headers for formats that support them
      if format == :record_batch_headers do
        assert message.headers == [%Kayrock.RecordBatch.RecordHeader{key: "1", value: "1"}]
      end
    end)
  end
end
