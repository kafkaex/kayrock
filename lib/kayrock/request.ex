defmodule Kayrock.InvalidRequestError do
  @moduledoc """
  Raised on serialization errors

  We catch errors in the serialization code so that we can provide a more
  helpful error message, including the original request struct
  """
  defexception [:message]

  @impl true
  def exception({orig_error, request}) do
    msg = "Error serializing request struct #{inspect(request)}: #{inspect(orig_error)}"
    %__MODULE__{message: msg}
  end
end

defprotocol Kayrock.Request do
  @type t :: Kayrock.Request.t()

  @spec serialize(t) :: iodata
  def serialize(struct)

  @spec api_vsn(t) :: non_neg_integer
  def api_vsn(struct)

  @spec response_deserializer(t) :: (binary -> {map, binary})
  def response_deserializer(struct)
end
