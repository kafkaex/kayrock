defmodule Kayrock.Compression.Zstd do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec

  alias Kayrock.Compression.Util

  @attr 4
  @default_level 3
  @min_level 1
  @max_level 22

  @compile {:no_warn_undefined, [:zstd, :ezstd]}

  @impl true
  def attr, do: @attr

  @impl true
  @spec available?() :: boolean
  def available? do
    has_stdlib_zstd?() or Code.ensure_loaded?(:ezstd)
  end

  @impl true
  @spec compress(binary) :: binary
  def compress(data), do: compress(data, @default_level)

  @impl true
  @spec compress(binary, pos_integer | nil) :: binary
  def compress(data, nil), do: compress(data)

  def compress(data, level) do
    do_compress(data, Util.clamp(level, @min_level, @max_level))
  end

  defp do_compress(data, level) do
    if has_stdlib_zstd?() do
      # OTP's zstd takes an options map keyed `compressionLevel` (an integer
      # argument raises FunctionClauseError) and returns an iolist.
      # https://www.erlang.org/doc/apps/stdlib/zstd.html#compress/2
      data |> :zstd.compress(%{compressionLevel: level}) |> IO.iodata_to_binary()
    else
      try do
        :ezstd.compress(data, level)
      rescue
        UndefinedFunctionError ->
          reraise "Zstd compression unavailable. Requires OTP 27+ or {:ezstd, \"~> 1.0\"}",
                  __STACKTRACE__
      end
    end
  end

  @impl true
  @spec decompress(binary) :: binary
  def decompress(data) do
    if has_stdlib_zstd?() do
      # `decompress(iodata()) -> iodata()` — callers here pattern-match on a
      # binary, and an iolist reaching the record decoder fails varint parsing.
      # https://www.erlang.org/doc/apps/stdlib/zstd.html#decompress/1
      data |> :zstd.decompress() |> IO.iodata_to_binary()
    else
      try do
        :ezstd.decompress(data)
      rescue
        UndefinedFunctionError ->
          reraise "Zstd compression unavailable. Requires OTP 27+ or {:ezstd, \"~> 1.0\"}",
                  __STACKTRACE__
      end
    end
  end

  defp has_stdlib_zstd? do
    Code.ensure_loaded?(:zstd) and function_exported?(:zstd, :compress, 2)
  end
end
