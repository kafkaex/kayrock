defmodule Kayrock.BrokerConnection do
  @moduledoc """
  A connection to a kafka broker.

  Implements the [Connection](https://github.com/fishcakez/connection/)
  behavior.

  You shouldn't generally need to use this module directly.  Use `Kayrock` or
  `Kayrock.Client`.
  """

  # raw connection to a kafka broker - this is based heavily on the connection
  # tcp example:
  # https://github.com/fishcakez/connection/tree/master/examples/tcp_connection
  # and KafkaEx's NetworkClient

  defmodule State do
    @moduledoc false
    defstruct([:host, :port, :opts, :is_ssl, :timeout, :sock])
  end

  use Connection

  alias Kayrock.Socket

  require Logger

  def start_link(host, port, opts, timeout \\ 5000) do
    Connection.start_link(__MODULE__, {host, port, opts, timeout})
  end

  def send(conn, data), do: Connection.call(conn, {:send, data})

  def recv(conn, timeout \\ 5000) do
    Connection.call(conn, {:recv, timeout})
  end

  def stop(conn) do
    close(conn)
    GenServer.stop(conn)
  end

  def close(conn), do: Connection.call(conn, :close)

  @impl true
  def init({host, port, opts, timeout}) do
    {:connect, :init,
     %State{
       host: format_host(host),
       port: port,
       opts: build_socket_options(Keyword.get(opts, :ssl, [])),
       is_ssl: Keyword.has_key?(opts, :ssl),
       timeout: timeout
     }}
  end

  @impl true
  def connect(
        _,
        %State{sock: nil} = s
      ) do
    case(Socket.create(s.host, s.port, s.opts, s.is_ssl)) do
      {:ok, socket} ->
        Logger.debug("Successfully connected to " <> describe_self(s, self()))

        {:ok, %{s | sock: socket}}

      err ->
        Logger.error(
          "Could not connect to #{describe_self(s, self())} because " <>
            "of error #{inspect(err)}"
        )

        {:backoff, 1000, s}
    end
  end

  @impl true
  def disconnect(info, %{sock: sock} = s) do
    :ok = Socket.close(sock)

    case info do
      {:close, from} ->
        Logger.debug("Disconnected from #{describe_self(s, self())}")
        Connection.reply(from, :ok)
        {:noconnect, %{s | sock: nil}}

      {:error, :closed} ->
        Logger.error("Connection was closed to #{describe_self(s, self())}")
        {:connect, :reconnect, %{s | sock: nil}}

      {:error, reason} ->
        reason = :inet.format_error(reason)

        Logger.error("Connection closed to #{describe_self(s, self())} because #{reason}")

        {:connect, :reconnect, %{s | sock: nil}}
    end
  end

  @impl true
  def handle_call(_, _, %{sock: nil} = s) do
    {:reply, {:error, :closed}, s}
  end

  def handle_call({:send, data}, _, %{sock: sock} = s) do
    case Socket.send(sock, data) do
      :ok ->
        {:reply, :ok, s}

      {:error, reason} ->
        Logger.error(
          "Sending to #{describe_self(s, self())} failed with reason #{inspect(reason)}"
        )

        {:disconnect, reason, reason, s}
    end
  end

  def handle_call({:recv, timeout}, _, %{sock: sock} = s) do
    case Socket.recv(sock, 0, timeout) do
      {:ok, data} ->
        {:reply, {:ok, data}, s}

      {:error, :timeout} ->
        {:reply, {:error, :timeout}, s}

      {:error, reason} ->
        Logger.log(
          :error,
          "Receiving data from #{describe_self(s, self())} failed with #{inspect(reason)}"
        )

        {:disconnect, {:error, reason}, s}
    end
  end

  def handle_call(:close, from, s) do
    {:disconnect, {:close, from}, s}
  end

  defp build_socket_options([]) do
    [:binary, {:packet, 4}, {:active, false}]
  end

  defp build_socket_options(ssl_options) do
    build_socket_options([]) ++ ssl_options
  end

  defp format_host(host) do
    case Regex.scan(~r/^(\d{1,3})\.(\d{1,3})\.(\d{1,3})\.(\d{1,3})$/, host) do
      [match_data] = [[_, _, _, _, _]] ->
        match_data
        |> tl
        |> List.flatten()
        |> Enum.map(&String.to_integer/1)
        |> List.to_tuple()

      # to_char_list is deprecated from Elixir 1.3 onward
      _ ->
        apply(String, :to_char_list, [host])
    end
  end

  defp describe_self(s, pid) do
    "broker #{inspect(s.host)}:#{inspect(s.port)} (#{inspect(pid)})"
  end
end
