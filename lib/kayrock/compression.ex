defmodule Kayrock.Compression do
  @moduledoc """
  Handles compression/decompression of messages.

  See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Compression

  To add new compression types:

  1. Add the appropriate dependency to mix.exs (don't forget to add it
  to the application list).
  2. Add the appropriate attribute value and compression_type atom.
  3. Add new Compression module.
  4. Add a decompress function in the new module.
  5. Add a compress function in the new module.

  ## Compression Levels

  - **Gzip**: 1 (fastest) to 9 (best compression), default: 6
  - **Snappy/LZ4**: No levels supported

  ## Dependencies

  - **Snappy**: `{:snappy, "~> 1.1"}` or `{:snappyer, "~> 1.2"}`
  - **LZ4**: `{:lz4b, "~> 0.2.0"}`

  """

  alias Kayrock.Compression.{Gzip, Snappy, Lz4}

  @type codec_t :: :gzip | :snappy | :lz4

  @codecs [
    gzip: Gzip,
    snappy: Snappy,
    lz4: Lz4
  ]

  @gzip_attr Gzip.attr()
  @snappy_attr Snappy.attr()
  @lz4_attr Lz4.attr()

  # ---------- Decompress ----------
  def decompress(@gzip_attr, data), do: Gzip.decompress(data)
  def decompress(@snappy_attr, data), do: Snappy.decompress(data)
  def decompress(@lz4_attr, data), do: Lz4.decompress(data)

  # ---------- Compress simple ----------
  def compress(:gzip, data), do: {Gzip.compress(data), Gzip.attr()}
  def compress(:snappy, data), do: {Snappy.compress(data), Snappy.attr()}
  def compress(:lz4, data), do: {Lz4.compress(data), Lz4.attr()}

  # ---------- Compress with opts ----------
  def compress(:gzip, data, opts) do
    level = Keyword.get(opts, :level)
    {Gzip.compress(data, level), Gzip.attr()}
  end

  def compress(:snappy, data, _opts), do: compress(:snappy, data)
  def compress(:lz4, data, _opts), do: compress(:lz4, data)

  @spec available_codecs() :: [codec_t]
  def available_codecs do
    for {name, mod} <- @codecs, mod.available?(), do: name
  end
end
