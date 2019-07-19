defmodule Kayrock.Macros do
  @moduledoc """
  Useful macros for kafka-related values

  `use Kayrock.Macros` is a convenient way to use these
  """

  defmacro __using__(_opts) do
    quote do
      import Kayrock.Macros
    end
  end

  @doc "The error code value indicating no error"
  defmacro no_error, do: 0

  @doc "Timestamp to supply for the latest offset"
  defmacro latest_offset, do: -1

  @doc "Timestamp to supply for the earliest offset"
  defmacro earliest_offset, do: -2

  @doc "No acks required on produce"
  defmacro no_acks, do: 0

  @doc "Only leader ack required on produce"
  defmacro leader_ack, do: 1

  @doc "Full ISR acks required on produce"
  defmacro full_isr_acks, do: -1
end
