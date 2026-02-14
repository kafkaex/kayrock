defmodule Kayrock.Compression.Snappy do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec

  @attr 2
  # Kafka's Snappy framing format magic bytes
  @kafka_snappy_magic <<130, 83, 78, 65, 80, 80, 89, 0>>

  @impl true
  def attr, do: @attr

  @impl true
  @spec available?() :: boolean
  def available? do
    Code.ensure_loaded?(:snappyer)
  end

  @impl true
  @spec compress(binary) :: binary
  def compress(data) do
    unless Code.ensure_loaded?(:snappyer) do
      raise """
      Snappy compression requires the snappyer dependency.

      Add to your mix.exs:

          {:snappyer, "~> 1.2"}

      Then run: mix deps.get
      """
    end

    {:ok, out} = :snappyer.compress(data)
    out
  end

  @impl true
  @spec decompress(binary) :: binary
  def decompress(data) do
    unless Code.ensure_loaded?(:snappyer) do
      raise """
      Snappy decompression requires the snappyer dependency.

      Add to your mix.exs:

          {:snappyer, "~> 1.2"}

      Then run: mix deps.get
      """
    end

    case data do
      # Kafka-specific chunked format with header
      <<@kafka_snappy_magic, _version::64, rest::binary>> ->
        decompress_chunks(rest, <<>>)

      # Raw snappy format
      _ ->
        case :snappyer.decompress(data) do
          {:ok, decompressed} -> decompressed
          {:error, reason} ->
            raise "Snappy decompression failed: #{inspect(reason)}"
        end
    end
  end

  defp decompress_chunks(<<>>, acc), do: acc

  defp decompress_chunks(<<0::32-signed, _rest::binary>>, acc), do: acc

  defp decompress_chunks(
         <<valsize::32-unsigned, value::size(valsize)-binary, rest::binary>>,
         acc
       ) do
    case :snappyer.decompress(value) do
      {:ok, decompressed} ->
        decompress_chunks(rest, acc <> decompressed)

      {:error, reason} ->
        raise "Snappy chunk decompression failed: #{inspect(reason)}"
    end
  end
end
