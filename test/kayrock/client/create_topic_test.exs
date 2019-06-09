defmodule Kayrock.Client.CreateTopicTest do
  # CreateTopics Request (Version: 3) => [create_topic_requests] timeout validate_only
  # create_topic_requests => topic num_partitions replication_factor [replica_assignment] [config_entries]
  # topic => STRING
  # num_partitions => INT32
  # replication_factor => INT16
  # replica_assignment => partition [replicas]
  #   partition => INT32
  #   replicas => INT32
  # config_entries => config_name config_value
  #   config_name => STRING
  #   config_value => NULLABLE_STRING
  # timeout => INT32
  # validate_only => BOOLEAN

  use Kayrock.ClientCase

  alias Kayrock.ErrorCode

  import Kayrock.Convenience

  test "delete topic - topic doesn't exist", %{client: client} do
    topic = unique_topic()
    refute topic_exists?(client, topic)

    {:ok, resp} = Kayrock.delete_topics(client, [topic])
    [topic_error_code] = resp.topic_error_codes
    assert topic_error_code[:error_code] == ErrorCode.unknown_topic()
  end

  test "create a topic - specify num partitions", %{client: client} do
    topic = unique_topic()
    refute topic_exists?(client, topic)

    {:ok, _} =
      Kayrock.create_topics(
        client,
        [%{topic: topic, num_partitions: 4, replication_factor: 2}],
        1000
      )

    assert topic_exists?(client, topic)

    {:ok, _} = Kayrock.delete_topics(client, [topic], 1000)
  end
end
