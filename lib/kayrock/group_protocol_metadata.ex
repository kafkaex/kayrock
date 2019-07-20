defmodule Kayrock.GroupProtocolMetadata do
  @moduledoc """
  Code to serialize consumer group ProtocolMetadata messages

  See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-JoinGroupRequest
  """

  alias Kayrock.Serialize

  defstruct version: 0, topics: [], user_data: ""

  def serialize(%__MODULE__{version: version, topics: topics, user_data: user_data}) do
    [
      Serialize.serialize(:int16, version),
      Serialize.serialize_array(:string, topics),
      Serialize.serialize(:bytes, user_data)
    ]
  end
end
