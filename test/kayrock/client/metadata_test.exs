defmodule Kayrock.Client.MetadataTest do
  use Kayrock.ClientCase

  alias Kayrock.ErrorCode

  test "works with an empty list of topics", %{client: client} do
    {:ok, metadata} = Kayrock.topics_metadata(client, [])
    assert metadata == []
  end

  test "lists all topics with a nil list", %{client: client} do
    {:ok, metadata} = Kayrock.topics_metadata(client, nil)
    topic = Enum.find(metadata, fn m -> m[:topic] == "test0p8p0" end)
    assert length(topic.partition_metadata) == 4
  end

  test "get metadata for a specific topic that exists", %{client: client} do
    {:ok, [topic]} = Kayrock.topics_metadata(client, ["test0p8p0"])
    assert topic[:topic] == "test0p8p0"
    assert length(topic.partition_metadata) == 4
  end

  test "request metadata for a topic that does not exist", %{client: client} do
    {:ok, [topic]} = Kayrock.topics_metadata(client, ["floogleflarp"])
    assert topic[:error_code] == ErrorCode.atom_to_code(:unknown_topic_or_partition)
  end
end
