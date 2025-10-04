defmodule Kayrock.Compression.Zstd do
  @moduledoc false
  @behaviour Kayrock.Compression.Codec

  alias Kayrock.Compression.Util

  @attr 4
  @default_level 3
  @min_level 1
  @max_level 22

  @compile {:no_warn_undefined, [:zstd]}
  @dialyzer {:nowarn_function, [do_compress: 2, decompress: 1]}

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
      :zstd.compress(data, level)
    else
      try do
        :ezstd.compress(data, level)
      rescue
        UndefinedFunctionError ->
          reraise "Zstd compression unavailable. Requires OTP 27+ or {:ezstd, \"~> 1.0\"}", __STACKTRACE__
      end
    end
  end

  @impl true
  @spec decompress(binary) :: binary
  def decompress(data) do
    if has_stdlib_zstd?() do
      :zstd.decompress(data)
    else
      try do
        :ezstd.decompress(data)
      rescue
        UndefinedFunctionError ->
          reraise "Zstd compression unavailable. Requires OTP 27+ or {:ezstd, \"~> 1.0\"}", __STACKTRACE__
      end
    end
  end

  defp has_stdlib_zstd? do
    function_exported?(:zstd, :compress, 2)
  end
end
