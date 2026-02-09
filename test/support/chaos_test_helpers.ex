defmodule Kayrock.ChaosTestHelpers do
  @moduledoc """
  Shared helper functions for chaos tests.
  """

  import ExUnit.Assertions
  import Kayrock.RequestFactory
  import Kayrock.IntegrationHelpers
  import Kayrock.ToxiproxyHelpers
  import Kayrock.TestSupport

  def build_test_records(count, value_content) do
    for i <- 1..count do
      %Kayrock.RecordBatch.Record{
        value: "#{value_content}-#{i}",
        key: nil,
        headers: []
      }
    end
  end

  def produce_compressed!(client, topic, records, api_version, compression) do
    compression_attrs = compression_to_attrs(compression)

    record_batch = %Kayrock.RecordBatch{
      records: records,
      attributes: compression_attrs
    }

    produce_request =
      produce_messages_request(topic, [[record_set: record_batch]], 1, api_version)

    {:ok, response} = Kayrock.client_call(client, produce_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses

    assert partition_response.error_code == 0,
           "Expected error_code=0 for #{compression} compression, got: #{partition_response.error_code}"

    partition_response.base_offset
  end

  def try_produce_compressed(client, topic, records, api_version, compression) do
    compression_attrs = compression_to_attrs(compression)

    record_batch = %Kayrock.RecordBatch{
      records: records,
      attributes: compression_attrs
    }

    produce_request =
      produce_messages_request(topic, [[record_set: record_batch]], 1, api_version)

    try do
      case Kayrock.client_call(client, produce_request, :controller) do
        {:ok, response} ->
          [topic_response] = response.responses
          [partition_response] = topic_response.partition_responses
          {:ok, partition_response}

        {:error, reason} ->
          {:error, reason}
      end
    catch
      kind, error ->
        {:exception, kind, error}
    end
  end

  def fetch_messages(client, topic, offset, api_version) do
    partition_data = [[topic: topic, partition: 0, fetch_offset: offset]]
    fetch_request = fetch_messages_request(partition_data, [], api_version)

    {:ok, response} = Kayrock.client_call(client, fetch_request, :controller)

    [topic_response] = response.responses
    assert topic_response.topic == topic

    [partition_response] = topic_response.partition_responses

    case partition_response.record_set do
      [%Kayrock.RecordBatch{records: records} | _] -> records
      _ -> []
    end
  end

  def find_coordinator_with_retry(client, group_id, api_version) do
    request = find_coordinator_request(group_id, api_version)

    {:ok, response} =
      Kayrock.TestSupport.with_retry(fn ->
        Kayrock.client_call(client, request, 1)
      end)

    response
  end

  def assert_valid_offset(offset, message) do
    assert is_integer(offset) and offset >= 0, "#{message} (got: #{inspect(offset)})"
    offset
  end

  def assert_message_count(records, expected_count, message) do
    actual_count = length(records)

    assert actual_count == expected_count,
           "#{message} (expected #{expected_count}, got #{actual_count})"

    records
  end

  def assert_offset_progression(offset1, offset2, context \\ "") do
    msg = if context != "", do: "#{context}: ", else: ""
    assert offset2 > offset1, "#{msg}Expected offset2 (#{offset2}) > offset1 (#{offset1})"
  end

  def assert_client_fails(operation) do
    result =
      try do
        operation.()
      catch
        _kind, _error -> :caught_exception
      end

    refute match?({:ok, _}, result),
           "Expected operation to fail, but got: #{inspect(result)}"
  end

  def assert_produce_failed(result, message) do
    case result do
      {:exception, _kind, _error} ->
        :ok

      {:error, _reason} ->
        :ok

      {:ok, %{error_code: 0}} ->
        flunk("#{message} - but produce succeeded with error_code=0")

      {:ok, %{error_code: _error_code}} ->
        :ok

      other ->
        flunk("#{message} - unexpected result: #{inspect(other)}")
    end
  end

  def setup_active_member(ctx, opts \\ []) do
    remove_all_toxics(ctx.toxiproxy, ctx.proxy_name)

    topic = create_topic(ctx.client, 5)
    group_id = "chaos-member-#{unique_string()}"

    coordinator = find_coordinator_with_retry(ctx.client, group_id, 2)
    assert coordinator.error_code == 0
    node_id = coordinator.node_id

    member_data = %{group_id: group_id, topics: [topic]}

    member_data =
      if opts[:session_timeout],
        do: Map.put(member_data, :session_timeout, opts[:session_timeout]),
        else: member_data

    join_request = join_group_request(member_data, 2)

    {:ok, join_response} =
      with_retry(fn ->
        Kayrock.client_call(ctx.client, join_request, node_id)
      end)

    assert join_response.error_code == 0
    assert is_binary(join_response.member_id)

    assignments = [
      %{
        member_id: join_response.member_id,
        topic: topic,
        partitions: [0, 1, 2]
      }
    ]

    sync_request = sync_group_request(group_id, join_response.member_id, assignments, 3)

    {:ok, sync_response} =
      with_retry(fn ->
        Kayrock.client_call(ctx.client, sync_request, node_id)
      end)

    assert sync_response.error_code == 0

    {group_id, join_response.member_id, join_response.generation_id, node_id}
  end

  defp compression_to_attrs(compression) do
    case compression do
      :gzip -> 1
      :snappy -> 2
      :lz4 -> 3
      :zstd -> 4
      :none -> 0
      _ -> 0
    end
  end
end
