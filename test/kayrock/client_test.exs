defmodule Kayrock.ClientTest do
  use ExUnit.Case

  @moduletag :integration

  defp unexpected_processes(p_after, p_before) do
    # returns a list of pids in p_after that were not in p_before _and also_ do
    # not correspond to registered process names; registered processes sometimes
    # spin up during normal operation and that doesn't mean we are leaking
    p_new = MapSet.difference(MapSet.new(p_after), MapSet.new(p_before))

    p_new
    |> Enum.map(fn pid ->
      case Process.info(pid, :registered_name) do
        {:registered_name, name} -> {pid, name}
        nil -> nil
      end
    end)
    |> Enum.reject(&is_nil/1)
    |> Enum.filter(fn
      {_, []} -> true
      _ -> false
    end)
  end

  test "connecting to/disconnecting from the brokers does not leak processes" do
    p_before = Process.list()

    {:ok, pid} = Kayrock.Client.start_link()
    assert Process.alive?(pid)

    Kayrock.Client.stop(pid)
    refute Process.alive?(pid)

    p_now = Process.list()

    assert unexpected_processes(p_now, p_before) == []
  end

  test "connecting to/doing an operation/disconnecting from brokers does not leak processes" do
    p_before = Process.list()

    {:ok, pid} = Kayrock.Client.start_link()
    assert Process.alive?(pid)

    {:ok, _} = Kayrock.api_versions(pid)

    Kayrock.Client.stop(pid)
    refute Process.alive?(pid)

    p_now = Process.list()

    assert unexpected_processes(p_now, p_before) == []
  end

  test "sudden death does not leak processes" do
    p_before = Process.list()

    {:ok, client} = Kayrock.Client.start_link()
    Process.unlink(client)
    Process.exit(client, :kill)

    refute Process.alive?(client)

    p_now = Process.list()

    assert unexpected_processes(p_now, p_before) == []
  end
end
