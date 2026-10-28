defmodule Kayrock.RaftVoterTest do
  @moduledoc """
  AddRaftVoter (key 80) v0-v1 and RemoveRaftVoter (key 81) v0, generated from
  the Kafka 4.4.0 JSON specs (flexible from v0). Admin-client APIs for
  dynamic KRaft quorums (KIP-853); brokers forward them to the controller.

  * AddRaftVoter v1 adds `AckWhenCommitted`
  * listener ports are `uint16`, voter directory ids are uuids

  Fixtures are derived by hand from the JSON specs.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.AddRaftVoter
  alias Kayrock.RemoveRaftVoter

  @dir_id <<0x0F0E0D0C0B0A09080706050403020100::128>>

  test "generated version ranges and api keys" do
    assert {AddRaftVoter.min_vsn(), AddRaftVoter.max_vsn()} == {0, 1}
    assert {RemoveRaftVoter.min_vsn(), RemoveRaftVoter.max_vsn()} == {0, 0}
    assert AddRaftVoter.V0.Request.api_key() == 80
    assert RemoveRaftVoter.V0.Request.api_key() == 81
  end

  defp add_voter_body do
    cstr("cluster-1") <>
      <<30_000::32, 2::32>> <>
      @dir_id <> carray([cstr("CONTROLLER") <> cstr("c2") <> <<9093::16>> <> tags()])
  end

  defp add_voter_fields do
    [
      cluster_id: "cluster-1",
      timeout_ms: 30_000,
      voter_id: 2,
      voter_directory_id: @dir_id,
      listeners: [%{name: "CONTROLLER", host: "c2", port: 9093}]
    ]
  end

  test "AddRaftVoter v0 request" do
    request =
      struct(
        AddRaftVoter.V0.Request,
        [correlation_id: 0, client_id: "kayrock"] ++ add_voter_fields()
      )

    assert serialize(request) == request_header(80, 0, 0, "kayrock") <> add_voter_body() <> tags()
  end

  test "AddRaftVoter v1 request adds ack_when_committed" do
    request =
      struct(
        AddRaftVoter.V1.Request,
        [correlation_id: 1, client_id: "kayrock", ack_when_committed: false] ++ add_voter_fields()
      )

    assert serialize(request) ==
             request_header(80, 1, 1, "kayrock") <> add_voter_body() <> <<0>> <> tags()
  end

  test "AddRaftVoter rejects a listener port outside uint16" do
    request =
      struct(
        AddRaftVoter.V0.Request,
        [correlation_id: 0, client_id: "kayrock"] ++
          Keyword.put(add_voter_fields(), :listeners, [%{name: "C", host: "h", port: 70_000}])
      )

    assert_raise Kayrock.InvalidRequestError, fn -> serialize(request) end
  end

  test "RemoveRaftVoter v0 request, with a null cluster id" do
    request = %RemoveRaftVoter.V0.Request{
      correlation_id: 0,
      client_id: "kayrock",
      cluster_id: nil,
      voter_id: 2,
      voter_directory_id: @dir_id
    }

    assert serialize(request) ==
             request_header(81, 0, 0, "kayrock") <> cstr(nil) <> <<2::32>> <> @dir_id <> tags()
  end

  for {module, version} <- [{AddRaftVoter, 0}, {AddRaftVoter, 1}, {RemoveRaftVoter, 0}] do
    test "#{inspect(module)} v#{version} response decodes" do
      module = unquote(module)
      version = unquote(version)
      response_module = Module.concat([module, :"V#{version}", Response])

      wire = response_header(9) <> <<0::32, 35::16>> <> cstr("kraft.version is 0") <> tags()

      assert {response, <<>>} = response_module.deserialize(wire)

      assert response == %{
               __struct__: response_module,
               correlation_id: 9,
               throttle_time_ms: 0,
               error_code: 35,
               error_message: "kraft.version is 0",
               tagged_fields: []
             }

      ok = response_header(9) <> <<0::32, 0::16>> <> cstr(nil) <> tags()
      assert {%{error_code: 0, error_message: nil}, <<>>} = module.deserialize(version, ok)
    end
  end
end
