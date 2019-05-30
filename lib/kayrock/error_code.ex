defmodule Kayrock.ErrorCode do
  @moduledoc """
  Utility for converting Kafka error codes to/from atom names
  """

  @known_codes -1..71
  @atom_to_code Enum.into(@known_codes, %{}, fn c -> {:kpro_schema.ec(c), c} end)

  require Logger

  def code_to_atom(code) when code in @known_codes do
    :kpro_schema.ec(code)
  end

  def code_to_atom(code) do
    Logger.warn("Unknown error code #{inspect(code)}")
    :unknown
  end

  def atom_to_code(atom) do
    Map.get(@atom_to_code, atom)
  end
end
