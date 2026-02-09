defmodule Kayrock.Compression.Lz4 do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec

  @compile {:no_warn_undefined, [:lz4b_frame]}
  @dialyzer {:nowarn_function, [compress: 1, decompress: 1]}

  @attr 3
  @impl true
  def attr, do: @attr

  @impl true
  @spec available?() :: boolean
  def available?, do: Code.ensure_loaded?(:lz4b_frame)

  @impl true
  @spec compress(binary) :: binary
  def compress(data) do
    unless Code.ensure_loaded?(:lz4b_frame) do
      raise """
      LZ4 compression requires the lz4b dependency.

      Add to your mix.exs:

          {:lz4b, "~> 0.0.13"}

      Then run: mix deps.get
      """
    end

    case :lz4b_frame.compress(data) do
      {:ok, compressed} -> compressed

      {:error, reason} ->
        raise "LZ4 compression failed: #{inspect(reason)}"
    end
  end

  @impl true
  @spec decompress(binary) :: binary
  def decompress(data) do
    unless Code.ensure_loaded?(:lz4b_frame) do
      raise """
      LZ4 decompression requires the lz4b dependency.

      Add to your mix.exs:

          {:lz4b, "~> 0.0.13"}

      Then run: mix deps.get
      """
    end

    case :lz4b_frame.decompress(data) do
      {:ok, decompressed} -> decompressed

      {:error, reason} ->
        raise "LZ4 decompression failed: #{inspect(reason)}"
    end
  end
end
