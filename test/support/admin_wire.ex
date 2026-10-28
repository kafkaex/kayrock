defmodule Kayrock.Test.AdminWire do
  @moduledoc """
  Builders for hand-derived wire fixtures of flexible (KIP-482) API versions.

  Each helper mirrors one rule of the Kafka protocol so fixtures can be written
  field by field straight from the JSON message specs, independently of the
  generated serializers:

    * `request_header/4` - request header v2 (`api_key`, `api_version`,
      `correlation_id`, non-compact nullable `client_id`, empty tag buffer)
    * `response_header/1` - response header v1 (`correlation_id`, empty tag
      buffer); ApiVersions responses use header v0, i.e. `<<correlation_id::32>>`
    * `uvarint/1` - unsigned varint
    * `cstr/1` - compact (nullable) string: `uvarint(len + 1)` then the bytes,
      `nil` is `uvarint(0)`
    * `carray/2` - compact (nullable) array: `uvarint(len + 1)` then each
      encoded element, `nil` is `uvarint(0)`
    * `tags/0` - an empty tag buffer
    * `tag/2` - one tagged field: `uvarint(tag)`, `uvarint(size)`, bytes
  """

  import Bitwise

  @spec request_header(non_neg_integer(), non_neg_integer(), integer(), String.t() | nil) ::
          binary()
  def request_header(api_key, api_version, correlation_id, client_id) do
    client =
      case client_id do
        nil -> <<-1::16-signed>>
        id -> <<byte_size(id)::16, id::binary>>
      end

    <<api_key::16, api_version::16, correlation_id::32, client::binary, 0>>
  end

  @spec response_header(integer()) :: binary()
  def response_header(correlation_id), do: <<correlation_id::32, 0>>

  @spec uvarint(non_neg_integer()) :: binary()
  def uvarint(n) when n < 0x80, do: <<n>>
  def uvarint(n), do: <<(n &&& 0x7F) ||| 0x80, uvarint(n >>> 7)::binary>>

  @spec cstr(String.t() | nil) :: binary()
  def cstr(nil), do: uvarint(0)
  def cstr(s), do: uvarint(byte_size(s) + 1) <> s

  @spec carray([term()] | nil, (term() -> binary())) :: binary()
  def carray(list, encode \\ & &1)
  def carray(nil, _encode), do: uvarint(0)

  def carray(list, encode) do
    IO.iodata_to_binary([uvarint(length(list) + 1) | Enum.map(list, encode)])
  end

  @spec tags() :: binary()
  def tags, do: <<0>>

  @spec tag(non_neg_integer(), binary()) :: binary()
  def tag(tag, value), do: uvarint(tag) <> uvarint(byte_size(value)) <> value

  @doc "Serializes a request struct to a binary"
  @spec serialize(struct()) :: binary()
  def serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))
end
