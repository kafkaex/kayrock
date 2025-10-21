defmodule Kayrock.Compression.Gzip do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec

  alias Kayrock.Compression.Util

  @attr 1
  
  @min_level 1
  @max_level 9

  @impl true
  def attr, do: @attr

  @impl true
  @spec available?() :: boolean
  def available?, do: true

  @impl true
  @spec compress(binary) :: binary
  def compress(data), do: :zlib.gzip(data)

  @impl true
  @spec compress(binary, integer | nil) :: binary
  def compress(data, nil), do: compress(data)
  def compress(data, level), do: gzip_with_level(data, level)

  @impl true
  @spec decompress(binary) :: binary
  def decompress(data), do: :zlib.gunzip(data)

  # Use zlib stream API to control gzip level (windowBits=31 => gzip)
  defp gzip_with_level(data, level) do
    z = :zlib.open()

    try do
      # level(1..9), method :deflated, windowBits 31 => gzip, memLevel 8, strategy :default
      :ok = :zlib.deflateInit(z, Util.clamp(level, @min_level, @max_level), :deflated, 31, 8, :default)
      z |> :zlib.deflate(data, :finish) |> IO.iodata_to_binary()
    after
      :zlib.close(z)
    end
  end
end
