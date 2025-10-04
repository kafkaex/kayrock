defmodule Kayrock.Compression.Codec do
  @moduledoc """
  Behaviour for all compression codecs used in Kayrock.
  """

  @callback attr() :: integer()
  @callback available?() :: boolean()
  @callback compress(binary()) :: binary()
  @callback decompress(binary()) :: binary()

  @callback compress(binary(), term()) :: binary()
  @optional_callbacks compress: 2
end
