defmodule Kayrock.Kafka4.RawCall do
  @moduledoc """
  Raw request/response round trips for `:kafka4` protocol tests.

  `call/2` sends a request on a `Kayrock.BrokerConnection` and decodes the
  response with the request's own deserializer, returning the decoded struct
  together with the bytes left over (which must be `<<>>` for a fully
  understood response) and the raw response binary (handy for capturing
  wire fixtures).
  """

  @doc """
  Sends `request` on `conn` and returns
  `{:ok, %{response: struct, rest: binary, raw: binary}}` or `{:error, reason}`.
  """
  @spec call(pid(), struct(), timeout()) ::
          {:ok, %{response: struct(), rest: binary(), raw: binary()}} | {:error, term()}
  def call(conn, request, timeout \\ 5_000) do
    with :ok <- Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request)),
         {:ok, raw} <- Kayrock.BrokerConnection.recv(conn, timeout) do
      {response, rest} = Kayrock.Request.response_deserializer(request).(raw)
      {:ok, %{response: response, rest: rest, raw: raw}}
    end
  end

  @doc """
  Like `call/3`, but asserts the response decoded with no leftover bytes and
  carries the request's correlation id, then returns the decoded struct.
  """
  @spec call!(pid(), struct(), timeout()) :: struct()
  def call!(conn, %{correlation_id: correlation_id} = request, timeout \\ 5_000) do
    case call(conn, request, timeout) do
      {:ok, %{response: %{correlation_id: ^correlation_id} = response, rest: <<>>}} ->
        response

      other ->
        raise ExUnit.AssertionError,
          message:
            "#{inspect(request.__struct__)}: expected a fully decoded response, got " <>
              inspect(other, limit: 20)
    end
  end
end
