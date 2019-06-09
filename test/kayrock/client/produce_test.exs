defmodule Kayrock.Client.ProduceTest do
  use Kayrock.ClientCase

  test "Simple produce works", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "simple_produce")
    {:ok, _} = Kayrock.produce(client, ["foo", "bar", "baz"], topic, 0)

    offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)
    IO.inspect(offset)

    {:ok, resp} = Kayrock.fetch(client, topic, 0, offset - 1)
    IO.inspect(resp)
  end
end
