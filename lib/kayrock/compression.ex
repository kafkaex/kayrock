defmodule Kayrock.Compression do
  @moduledoc """
  Handles compression/decompression of messages.

  NOTE this is a copy of KafkaEx.Compression:
  https://github.com/kafkaex/kafka_ex/blob/master/lib/kafka_ex/compression.ex

  It is duplicated here to avoid creating a circular dependency.

  See https://cwiki.apache.org/confluence/display/KAFKA/A+Guide+To+The+Kafka+Protocol#AGuideToTheKafkaProtocol-Compression

  To add new compression types:

  1. Add the appropriate dependency to mix.exs (don't forget to add it
  to the application list).
  2. Add the appropriate attribute value and compression_type atom.
  3. Add a decompress function clause.
  4. Add a compress function clause.

  """

  @gzip_attribute 1
  @snappy_attribute 2

  @type attribute_t :: integer
  @type compression_type_t :: :snappy | :gzip

  @doc """
  This function should pattern match on the attribute value and return
  the decompressed data.
  """
  @spec decompress(attribute_t, binary) :: binary
  def decompress(@gzip_attribute, <<window_size::8-signed, _::bits>> = data) do
    z = :zlib.open()
    :zlib.inflateInit(z, window_size)
    [v | _] = :zlib.inflate(z, data)
    v
  end

  def decompress(@snappy_attribute, data) do
    case data do
      <<130, "SNAPPY", 0, _snappy_version_info::64, rest::binary>> ->
        snappy_decompress_chunk(rest, <<>>)

      _ ->
        {:ok, decompressed_value} = snappy_module().decompress(data)
        decompressed_value
    end
  end

  defp snappy_module do
    Application.get_env(:kayrock, :snappy_module)
  end

  @doc """
  This function should pattern match on the compression_type atom and
  return the compressed data as well as the corresponding attribute
  value.
  """
  @spec compress(compression_type_t, iodata) :: {binary, attribute_t}
  def compress(:snappy, data) do
    {:ok, compressed_data} = snappy_module().compress(data)
    {compressed_data, @snappy_attribute}
  end

  def compress(:gzip, data) do
    compressed_data = :zlib.gzip(data)
    {compressed_data, @gzip_attribute}
  end

  def snappy_decompress_chunk(<<>>, so_far) do
    so_far
  end

  def snappy_decompress_chunk(<<0::32-signed, _rest::bits>>, so_far) do
    so_far
  end

  def snappy_decompress_chunk(
        <<valsize::32-unsigned, value::size(valsize)-binary, rest::bits>>,
        so_far
      ) do
    {:ok, decompressed_value} = snappy_module().decompress(value)
    snappy_decompress_chunk(rest, so_far <> decompressed_value)
  end
end
