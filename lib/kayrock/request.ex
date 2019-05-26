defprotocol Kayrock.Request do
  @type t :: Kayrock.Request.t()

  @spec serialize(t) :: iodata
  def serialize(struct)

  @spec api_vsn(t) :: non_neg_integer
  def api_vsn(struct)

  @spec response_deserializer(t) :: (binary -> {map, binary})
  def response_deserializer(struct)
end
