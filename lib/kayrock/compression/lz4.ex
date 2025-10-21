defmodule Kayrock.Compression.Lz4 do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec
  
  # Kafka expects LZ4 Frame (KIP-57). :lz4b_frame handles this framing.
  @attr 3
  
  @impl true
  def attr, do: @attr
  
  @impl true
  @spec available?() :: boolean
  def available?, do: Code.ensure_loaded?(:lz4b_frame)
  
  @impl true
  @spec compress(binary) :: binary
  def compress(data) do
    case :lz4b_frame.compress(data) do
      {:ok, compressed} -> compressed
      {:error, reason} ->
        raise "LZ4 compression failed: #{inspect(reason)}"
    end
  rescue
    UndefinedFunctionError ->
      reraise "LZ4 compression unavailable. Requires {:lz4b, \"~> 0.2\"}", 
              __STACKTRACE__
  end
  
  @impl true
  @spec decompress(binary) :: binary
  def decompress(data) do
    case :lz4b_frame.decompress(data) do
      {:ok, decompressed} -> decompressed
      {:error, reason} ->
        raise "LZ4 decompression failed: #{inspect(reason)}"
    end
  rescue
    UndefinedFunctionError ->
      reraise "LZ4 compression unavailable. Requires {:lz4b, \"~> 0.2\"}", 
              __STACKTRACE__
  end
end
