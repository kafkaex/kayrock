defmodule Kayrock.AlterPartitionReassignmentsJsonVersionsTest do
  @moduledoc """
  AlterPartitionReassignments (key 45) v1, generated from the Kafka 4.4.0
  JSON specs.

  * v1: the request and response add `AllowReplicationFactorChange` (KIP-860)
  * a null `Replicas` array cancels a pending reassignment

  Fixtures are derived by hand from
  `AlterPartitionReassignments{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.AlterPartitionReassignments, as: APR

  test "v0-v1 are generated" do
    assert {APR.min_vsn(), APR.max_vsn()} == {0, 1}
  end

  test "v1 request serializes, including a null (cancel) replica list" do
    request = %APR.V1.Request{
      correlation_id: 1,
      client_id: "kayrock",
      timeout_ms: 60_000,
      allow_replication_factor_change: false,
      topics: [
        %{
          name: "t1",
          partitions: [
            %{partition_index: 0, replicas: [1, 2, 3]},
            %{partition_index: 1, replicas: nil}
          ]
        }
      ]
    }

    assert serialize(request) ==
             request_header(45, 1, 1, "kayrock") <>
               <<60_000::32, 0>> <>
               carray([
                 cstr("t1") <>
                   carray([
                     <<0::32>> <> carray([<<1::32>>, <<2::32>>, <<3::32>>]) <> tags(),
                     <<1::32>> <> carray(nil) <> tags()
                   ]) <> tags()
               ]) <> tags()
  end

  test "v1 response decodes per the JSON spec" do
    wire =
      response_header(1) <>
        <<0::32, 1, 0::16>> <>
        cstr(nil) <>
        carray([
          cstr("t1") <>
            carray([
              <<0::32, 0::16>> <> cstr(nil) <> tags(),
              <<1::32, 85::16>> <> cstr("no reassignment in progress") <> tags()
            ]) <> tags()
        ]) <> tags()

    assert {response, <<>>} = APR.V1.Response.deserialize(wire)

    assert response == %APR.V1.Response{
             correlation_id: 1,
             throttle_time_ms: 0,
             allow_replication_factor_change: 1,
             error_code: 0,
             error_message: nil,
             responses: [
               %{
                 name: "t1",
                 partitions: [
                   %{partition_index: 0, error_code: 0, error_message: nil, tagged_fields: []},
                   %{
                     partition_index: 1,
                     error_code: 85,
                     error_message: "no reassignment in progress",
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }

    assert APR.deserialize(1, wire) == {response, <<>>}
  end
end
