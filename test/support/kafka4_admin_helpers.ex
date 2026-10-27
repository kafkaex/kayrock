defmodule Kayrock.Kafka4AdminHelpers do
  @moduledoc """
  Raw request helpers for the topic / cluster admin `:kafka4` tests
  (`test/integration/kafka4/`).

  `raw_call/3` sends one request on a fresh `Kayrock.BrokerConnection`, so
  the exact request version is used (no client negotiation), and returns
  the decoded response together with any bytes the deserializer left over;
  tests assert that is `<<>>` to prove the response decoded fully.
  """

  import Kayrock.Kafka4Helpers, only: [connect_broker: 1]

  alias Testcontainers.Container

  @doc "The KRaft cluster id `Kayrock.Kafka4Container` formats storage with"
  @spec cluster_id() :: String.t()
  def cluster_id, do: Kayrock.Kafka4Container.new().cluster_id

  @doc "The single node's id (broker and controller)"
  @spec node_id() :: pos_integer()
  def node_id, do: 1

  @doc """
  Sends `request` (correlation id and client id are filled in) and returns
  `{response, rest}` where `rest` is whatever the deserializer did not
  consume. Raises if the broker does not answer.
  """
  @spec raw_call(Container.t(), struct(), pos_integer()) :: {map(), binary()}
  def raw_call(kafka, request, timeout \\ 15_000) do
    {:ok, conn} = connect_broker(kafka)
    correlation_id = rem(:erlang.unique_integer([:positive]), 0x7FFFFFFF)
    request = %{request | correlation_id: correlation_id, client_id: "kayrock-kafka4"}

    try do
      :ok = Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request))
      {:ok, data} = Kayrock.BrokerConnection.recv(conn, timeout)

      {%{correlation_id: ^correlation_id} = response, rest} =
        Kayrock.Request.response_deserializer(request).(data)

      {response, rest}
    after
      Kayrock.BrokerConnection.stop(conn)
    end
  end
end
