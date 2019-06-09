defimpl Kayrock.Request, for: Kayrock.RecordBatch do
  alias Kayrock.RecordBatch

  defdelegate serialize(record_batch), to: RecordBatch

  def api_vsn(%RecordBatch{}), do: 2

  def response_deserializer(%RecordBatch{}) do
    fn data -> {RecordBatch.deserialize(data), <<>>} end
  end
end

defimpl Kayrock.Request, for: Kayrock.MessageSet do
  alias Kayrock.MessageSet

  defdelegate serialize(message_set), to: MessageSet

  def api_vsn(%MessageSet{}), do: 1

  def response_deserializer(%MessageSet{}) do
    fn data -> {MessageSet.deserialize(data), <<>>} end
  end
end
