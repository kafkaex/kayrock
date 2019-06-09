defmodule Kayrock.Client.ProduceTest do
  use Kayrock.ClientCase

  alias Kayrock.RecordBatch

  test "Simple produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")

    record_batch = RecordBatch.from_binary_list(["foo", "bar", "baz"])
    {:ok, resp} = Kayrock.produce(client, record_batch, topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)
    # IO.inspect(offset)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)
    # IO.inspect(resp)
  end
end
