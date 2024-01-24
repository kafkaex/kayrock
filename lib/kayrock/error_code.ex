defmodule Kayrock.ErrorCode do
  @moduledoc """
  Utility for converting Kafka error codes to/from atom names

  Note the actual error code values are determined by `:kpro_schema.c/1`.
  Currently covering the range -1 to 71.

  See https://kafka.apache.org/protocol#protocol_error_codes
  """

  defmodule InvalidErrorCodeException do
    @moduledoc """
    Raised if `Kayrock.ErrorCode.code_to_atom!/1` is called with a code that
    does not correspond to a known Kafka error code.
    """
    defexception [:message]

    @impl true
    def exception(code) do
      %__MODULE__{message: "Invalid error code: #{inspect(code)}"}
    end
  end

  defmodule InvalidAtomException do
    @moduledoc """
    Raised if `Kayrock.ErrorCode.atom_to_code!/1` is called with an atom that
    does not correspond to a known Kafka error code.
    """
    defexception [:message]

    @impl true
    def exception(atom) do
      %__MODULE__{message: "Invalid error atom: #{inspect(atom)}"}
    end
  end

  alias Kayrock.KafkaSchemaMetadata

  @known_codes -1..71
  case Code.ensure_compiled(KafkaSchemaMetadata) do
    {:module, _} ->
      @atom_to_code Enum.into(@known_codes, %{}, fn c ->
                      {KafkaSchemaMetadata.error_code_to_error(c), c}
                    end)
  end

  @typedoc """
  A numeric Kafka error code
  """
  @type error_code :: integer

  @typedoc """
  An erlang/elixir atom representation of an error code
  """
  @type error_atom :: atom

  require Logger

  @doc """
  Converts an error code to an atom

  An unknown code results in a return of `:unknown`
  """
  @spec code_to_atom(error_code) :: error_atom
  def code_to_atom(code) when code in @known_codes do
    KafkaSchemaMetadata.error_code_to_error(code)
  end

  def code_to_atom(code) do
    Logger.warning("Unknown error code #{inspect(code)}")
    :unknown
  end

  @doc """
  Converts an error code to an atom, raising an
  `Kayrock.ErrorCode.InvalidErrorCodeException` if an unknown error code is supplied
  """
  @spec code_to_atom!(error_code) :: error_atom
  def code_to_atom!(code) do
    case code_to_atom(code) do
      :unknown -> raise InvalidErrorCodeException, code
      v -> v
    end
  end

  @doc """
  Converts an atom to a numeric error code

  Returns nil if an unknown atom is supplied
  """
  @spec atom_to_code(error_atom) :: error_code | nil
  def atom_to_code(atom) do
    Map.get(@atom_to_code, atom)
  end

  @doc """
  Converts an atom to a numeric error code, raising an
  `Kayrock.ErrorCode.InvalidAtomException` if an unknown atom is supplied
  """
  @spec atom_to_code!(error_atom) :: error_code
  def atom_to_code!(atom) do
    case atom_to_code(atom) do
      nil -> raise InvalidAtomException, atom
      v -> v
    end
  end

  ######################################################################
  # convenience functions
  @spec unknown_topic() :: error_code
  def unknown_topic, do: atom_to_code!(:unknown_topic_or_partition)
end
