defmodule Kayrock.Kafka4.RaftVoterTest do
  @moduledoc """
  AddRaftVoter v0-v1 and RemoveRaftVoter v0 against apache/kafka:4.4.0.

  The harness runs a single node with a static quorum
  (`controller.quorum.voters`, `kraft.version` 0), so the controller answers
  with UNSUPPORTED_VERSION (35) or INVALID_REQUEST (42): actually changing
  the voter set needs a dynamic quorum (`kraft.version` 1, formatted with
  `--standalone` / `--initial-controllers`) and a second controller. These
  tests prove the broker accepts and forwards each version and that the
  responses decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @unsupported_version 35
  @invalid_request 42

  defp listeners, do: [%{name: "CONTROLLER", host: "localhost", port: 19_093}]

  test "AddRaftVoter v0", %{kafka: kafka} do
    request = %Kayrock.AddRaftVoter.V0.Request{
      cluster_id: cluster_id(),
      timeout_ms: 5_000,
      voter_id: 2,
      voter_directory_id: <<1::128>>,
      listeners: listeners()
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code in [@unsupported_version, @invalid_request]
    assert response.error_message =~ "kraft.version"
  end

  test "AddRaftVoter v1", %{kafka: kafka} do
    request = %Kayrock.AddRaftVoter.V1.Request{
      cluster_id: cluster_id(),
      timeout_ms: 5_000,
      voter_id: 2,
      voter_directory_id: <<1::128>>,
      listeners: listeners(),
      ack_when_committed: true
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == @unsupported_version
    assert response.error_message =~ "kraft.version"
  end

  test "AddRaftVoter v1 without the controller listener is rejected", %{kafka: kafka} do
    request = %Kayrock.AddRaftVoter.V1.Request{
      cluster_id: nil,
      timeout_ms: 5_000,
      voter_id: 2,
      voter_directory_id: <<1::128>>,
      listeners: [],
      ack_when_committed: false
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == @invalid_request
    assert is_binary(response.error_message)
  end

  test "RemoveRaftVoter v0", %{kafka: kafka} do
    request = %Kayrock.RemoveRaftVoter.V0.Request{
      cluster_id: cluster_id(),
      voter_id: 2,
      voter_directory_id: <<1::128>>
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == @unsupported_version
    assert response.error_message =~ "kraft.version"
  end
end
