defmodule Kayrock.Client.ListOffsetsTest do
  use Kayrock.ClientCase

  alias Kayrock.RecordBatch

  test "list offsets for a single partition", %{client: client} do
    {:ok, topic} = ensure_test_topic(client, "list_offsets_test")

    first_offset = Kayrock.Convenience.partition_last_offset(client, topic, 0)

    assert first_offset >= 0

    record_batch = RecordBatch.from_binary_list(["one", "two", "three"])
    {:ok, _} = Kayrock.produce(client, record_batch, topic, 0)

    offset =
      fn ->
        :timer.sleep(10)
        Kayrock.Convenience.partition_last_offset(client, topic, 0)
      end
      |> Stream.repeatedly()
      |> Stream.drop_while(fn x -> x <= first_offset end)
      |> Enum.take(1)
      |> List.last()

    assert offset == first_offset + 3
  end
end
