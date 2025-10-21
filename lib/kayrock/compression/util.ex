defmodule Kayrock.Compression.Util do
  @moduledoc false

  @spec clamp(integer, integer, integer) :: integer
  def clamp(v, lo, hi), do: v |> max(lo) |> min(hi)
end
