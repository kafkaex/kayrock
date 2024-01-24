defmodule Kayrock.RequestFactory do
  @moduledoc """
  Documentation for Kayrock.RequestFactory.
  """

  @doc """
  Creates a request to create a topic
  Uses min of api_version and max supported version
  """
  def create_topic_request(topic_name, api_version) do
    api_version = min(Kayrock.CreateTopics.max_vsn(), api_version)
    request = Kayrock.CreateTopics.get_request_struct(api_version)

    topic_config = %{
      topic: topic_name,
      num_partitions: 3,
      replication_factor: 1,
      replica_assignment: [],
      config_entries: []
    }

    %{request | create_topic_requests: [topic_config], timeout: 1000}
  end

  @doc """
  Creates a request to produce a message to a topic
  Uses min of api_version and max supported version
  """
  def produce_messages_request(topic_name, data, ack, api_version) do
    api_version = min(Kayrock.Produce.max_vsn(), api_version)
    request = Kayrock.Produce.get_request_struct(api_version)

    topic_data = [
      %{
        topic: topic_name,
        data:
          Enum.map(data, fn datum ->
            %{
              partition: Keyword.get(datum, :partition, 0),
              record_set: Keyword.get(datum, :record_set, [])
            }
          end)
      }
    ]

    %{request | topic_data: topic_data, acks: ack, timeout: 1000}
  end

  @doc """
  Creates a request to fetch messages from a topic
  Uses min of api_version and max supported version
  """
  def fetch_messages_request(partition_data, opts, api_version) do
    api_version = min(Kayrock.Fetch.max_vsn(), api_version)
    request = Kayrock.Fetch.get_request_struct(api_version)
    max_bytes = Keyword.get(opts, :max_bytes, 1_000_000)

    request_date = %{
      replica_id: -1,
      max_wait_time: Keyword.get(opts, :max_wait_time, 1000),
      min_bytes: Keyword.get(opts, :min_bytes, 0),
      max_bytes: Keyword.get(opts, :max_bytes, max_bytes),
      isolation_level: Keyword.get(opts, :isolation_level, 1),
      session_id: Keyword.get(opts, :session_id, 0),
      epoch: Keyword.get(opts, :epoch, 0),
      topics:
        Enum.map(partition_data, fn partition ->
          %{
            topic: Keyword.fetch!(partition, :topic),
            partitions: [
              %{
                partition: Keyword.get(partition, :partition, 0),
                fetch_offset: Keyword.get(partition, :fetch_offset, 0),
                max_bytes: Keyword.get(partition, :max_bytes, max_bytes),
                log_start_offset: Keyword.get(partition, :log_start_offset, 0)
              }
            ]
          }
        end)
    }

    %{Map.merge(request, request_date) | replica_id: -1}
  end
end
