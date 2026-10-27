defmodule Kayrock.DescribeQuorumTest do
  @moduledoc """
  DescribeQuorum (key 55) v0-v2, generated from the Kafka 4.4.0 JSON specs
  (flexible from v0).

  * v1: `ReplicaState` adds `LastFetchTimestamp` and `LastCaughtUpTimestamp`
  * v2: top-level and per-partition `ErrorMessage`, `ReplicaDirectoryId`
    (uuid) and a `Nodes` array with `uint16` listener ports (KIP-853)

  The request is identical in every version. Fixtures are derived by hand
  from `DescribeQuorum{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.DescribeQuorum

  @dir_id <<0x11223344556677889900AABBCCDDEEFF::128>>

  test "v0-v2 are generated with api key 55" do
    assert {DescribeQuorum.min_vsn(), DescribeQuorum.max_vsn()} == {0, 2}
    assert DescribeQuorum.V0.Request.api_key() == 55
  end

  for version <- 0..2 do
    test "v#{version} request serializes topics and partitions" do
      version = unquote(version)

      request =
        struct(Module.concat([DescribeQuorum, :"V#{version}", Request]),
          correlation_id: version,
          client_id: "kayrock",
          topics: [%{topic_name: "__cluster_metadata", partitions: [%{partition_index: 0}]}]
        )

      assert serialize(request) ==
               request_header(55, version, version, "kayrock") <>
                 carray([cstr("__cluster_metadata") <> carray([<<0::32>> <> tags()]) <> tags()]) <>
                 tags()
    end
  end

  defp partition_prefix(error_message) do
    <<0::32, 0::16>> <> error_message <> <<1::32, 3::32, 19::64>>
  end

  test "v0 response decodes voters and observers" do
    wire =
      response_header(0) <>
        <<0::16>> <>
        carray([
          cstr("__cluster_metadata") <>
            carray([
              partition_prefix(<<>>) <>
                carray([<<1::32, 19::64>> <> tags()]) <>
                carray([<<2::32, 17::64>> <> tags()]) <> tags()
            ]) <> tags()
        ]) <> tags()

    assert {response, <<>>} = DescribeQuorum.V0.Response.deserialize(wire)

    assert response == %DescribeQuorum.V0.Response{
             correlation_id: 0,
             error_code: 0,
             topics: [
               %{
                 topic_name: "__cluster_metadata",
                 partitions: [
                   %{
                     partition_index: 0,
                     error_code: 0,
                     leader_id: 1,
                     leader_epoch: 3,
                     high_watermark: 19,
                     current_voters: [%{replica_id: 1, log_end_offset: 19, tagged_fields: []}],
                     observers: [%{replica_id: 2, log_end_offset: 17, tagged_fields: []}],
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }
  end

  test "v1 response decodes fetch / caught-up timestamps" do
    wire =
      response_header(1) <>
        <<0::16>> <>
        carray([
          cstr("__cluster_metadata") <>
            carray([
              partition_prefix(<<>>) <>
                carray([<<1::32, 19::64, -1::64-signed, 1_793_130_801_808::64>> <> tags()]) <>
                carray([]) <> tags()
            ]) <> tags()
        ]) <> tags()

    assert {response, <<>>} = DescribeQuorum.V1.Response.deserialize(wire)

    assert [%{partitions: [%{current_voters: [voter], observers: []}]}] = response.topics

    assert voter == %{
             replica_id: 1,
             log_end_offset: 19,
             last_fetch_timestamp: -1,
             last_caught_up_timestamp: 1_793_130_801_808,
             tagged_fields: []
           }
  end

  test "v2 response decodes error messages, directory ids and nodes" do
    wire =
      response_header(2) <>
        <<0::16>> <>
        cstr(nil) <>
        carray([
          cstr("__cluster_metadata") <>
            carray([
              partition_prefix(cstr("")) <>
                carray([<<1::32>> <> @dir_id <> <<19::64, 5::64, 6::64>> <> tags()]) <>
                carray([]) <> tags()
            ]) <> tags()
        ]) <>
        carray([
          <<1::32>> <>
            carray([cstr("CONTROLLER") <> cstr("localhost") <> <<65_535::16>> <> tags()]) <>
            tags()
        ]) <> tags()

    assert {response, <<>>} = DescribeQuorum.V2.Response.deserialize(wire)

    assert response == %DescribeQuorum.V2.Response{
             correlation_id: 2,
             error_code: 0,
             error_message: nil,
             topics: [
               %{
                 topic_name: "__cluster_metadata",
                 partitions: [
                   %{
                     partition_index: 0,
                     error_code: 0,
                     error_message: "",
                     leader_id: 1,
                     leader_epoch: 3,
                     high_watermark: 19,
                     current_voters: [
                       %{
                         replica_id: 1,
                         replica_directory_id: @dir_id,
                         log_end_offset: 19,
                         last_fetch_timestamp: 5,
                         last_caught_up_timestamp: 6,
                         tagged_fields: []
                       }
                     ],
                     observers: [],
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ],
             nodes: [
               %{
                 node_id: 1,
                 listeners: [
                   %{name: "CONTROLLER", host: "localhost", port: 65_535, tagged_fields: []}
                 ],
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }

    assert DescribeQuorum.deserialize(2, wire) == {response, <<>>}
  end
end
