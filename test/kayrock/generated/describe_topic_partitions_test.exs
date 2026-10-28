defmodule Kayrock.DescribeTopicPartitionsTest do
  @moduledoc """
  DescribeTopicPartitions (key 75) v0, generated from the Kafka 4.4.0 JSON
  specs (flexible).

  Exercises the nullable `Cursor` / `NextCursor` structs (int8 -1 when null,
  1 followed by the fields when present), uuid topic ids and the nullable
  `EligibleLeaderReplicas` / `LastKnownElr` arrays.

  Fixtures are derived by hand from
  `DescribeTopicPartitions{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.DescribeTopicPartitions, as: DTP

  @topic_id <<0xA0A6E003C2634A1EB9A75463D296C7EC::128>>

  test "v0 is generated with api key 75" do
    assert {DTP.min_vsn(), DTP.max_vsn()} == {0, 0}
    assert DTP.V0.Request.api_key() == 75
  end

  describe "request" do
    test "without a cursor" do
      request = %DTP.V0.Request{
        correlation_id: 1,
        client_id: "kayrock",
        topics: [%{name: "t1"}, %{name: "t2"}],
        response_partition_limit: 2000,
        cursor: nil
      }

      assert serialize(request) ==
               request_header(75, 0, 1, "kayrock") <>
                 carray([cstr("t1") <> tags(), cstr("t2") <> tags()]) <>
                 <<2000::32, -1::8-signed>> <> tags()
    end

    test "with a cursor" do
      request = %DTP.V0.Request{
        correlation_id: 1,
        client_id: "kayrock",
        topics: [%{name: "t1"}],
        response_partition_limit: 1,
        cursor: %{topic_name: "t1", partition_index: 3}
      }

      assert serialize(request) ==
               request_header(75, 0, 1, "kayrock") <>
                 carray([cstr("t1") <> tags()]) <>
                 <<1::32, 1>> <> cstr("t1") <> <<3::32>> <> tags() <> tags()
    end
  end

  describe "response" do
    defp partition_wire(index, elr) do
      <<0::16, index::32, 1::32, 0::32>> <>
        carray([<<1::32>>]) <>
        carray([<<1::32>>]) <>
        carray(elr) <> carray(elr) <> carray([]) <> tags()
    end

    test "decodes topics, partitions and a next cursor" do
      wire =
        response_header(1) <>
          <<0::32>> <>
          carray([
            <<0::16>> <>
              cstr("t1") <>
              @topic_id <>
              <<0>> <>
              carray([partition_wire(0, nil), partition_wire(1, [<<2::32>>])]) <>
              <<3576::32>> <> tags()
          ]) <> <<1>> <> cstr("t1") <> <<2::32>> <> tags() <> tags()

      assert {response, <<>>} = DTP.V0.Response.deserialize(wire)

      partition = fn index, elr ->
        %{
          error_code: 0,
          partition_index: index,
          leader_id: 1,
          leader_epoch: 0,
          replica_nodes: [1],
          isr_nodes: [1],
          eligible_leader_replicas: elr,
          last_known_elr: elr,
          offline_replicas: [],
          tagged_fields: []
        }
      end

      assert response == %DTP.V0.Response{
               correlation_id: 1,
               throttle_time_ms: 0,
               topics: [
                 %{
                   error_code: 0,
                   name: "t1",
                   topic_id: @topic_id,
                   is_internal: 0,
                   partitions: [partition.(0, nil), partition.(1, [2])],
                   topic_authorized_operations: 3576,
                   tagged_fields: []
                 }
               ],
               next_cursor: %{topic_name: "t1", partition_index: 2, tagged_fields: []},
               tagged_fields: []
             }
    end

    test "decodes an unknown topic with a null name and no next cursor" do
      wire =
        response_header(2) <>
          <<0::32>> <>
          carray([
            <<3::16>> <>
              cstr(nil) <>
              <<0::128>> <> <<0>> <> carray([]) <> <<-2_147_483_648::32-signed>> <> tags()
          ]) <> <<-1::8-signed>> <> tags()

      assert {response, <<>>} = DTP.V0.Response.deserialize(wire)

      assert [%{error_code: 3, name: nil, topic_id: <<0::128>>, partitions: []}] =
               response.topics

      assert response.next_cursor == nil
      assert DTP.deserialize(0, wire) == {response, <<>>}
    end
  end
end
