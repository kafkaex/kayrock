defmodule Kayrock.OffsetFetchMemberEpochTest do
  @moduledoc """
  OffsetFetch v7-v10 (JSON-sourced; v0-v6 come from `:kpro_schema` and are
  covered by `Kayrock.OffsetFetchTest`).

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `OffsetFetchRequest.json` / `OffsetFetchResponse.json` specs:

    * v7 adds `require_stable`;
    * v8 batches several groups (`groups[]` replaces `group_id` / `topics`);
    * v9 adds the KIP-848 `member_id` / `member_epoch` per group;
    * v10 replaces the topic `name` with `topic_id` (uuid).
  """
  use ExUnit.Case, async: true

  alias Kayrock.OffsetFetch

  @topic_id <<0xAB, 0xCD, 0xEF, 0x01, 0x23, 0x45, 0x46, 0x67, 0x89, 0xAB, 0xCD, 0xEF, 0x01, 0x23,
              0x45, 0x67>>

  defp request_header(vsn), do: <<9::16, vsn::16, 13::32, 7::16, "kayrock", 0>>

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  # partitions [{0, offset 42, leader epoch 5, metadata "md", error 0}] + topic tag buffer
  @partition_bytes <<2, 0::32, 42::64, 5::32, 3, "md", 0::16, 0, 0>>

  @partition %{
    partition_index: 0,
    committed_offset: 42,
    committed_leader_epoch: 5,
    metadata: "md",
    error_code: 0,
    tagged_fields: []
  }

  test "version range and dispatch" do
    assert OffsetFetch.max_vsn() == 10

    for vsn <- 7..10 do
      assert Kayrock.Request.api_vsn(OffsetFetch.get_request_struct(vsn)) == vsn
    end
  end

  describe "requests" do
    test "V7 adds require_stable" do
      request = %OffsetFetch.V7.Request{
        correlation_id: 13,
        client_id: "kayrock",
        group_id: "g1",
        topics: [%{name: "t", partition_indexes: [0, 2], tagged_fields: []}],
        require_stable: true
      }

      expected =
        request_header(7) <>
          <<3, "g1", 2, 2, "t", 3, 0::32, 2::32, 0>> <>
          <<
            # require_stable, tag buffer
            1,
            0
          >>

      assert serialize(request) == expected
    end

    test "V8 batches groups (null topics = all topics)" do
      request = %OffsetFetch.V8.Request{
        correlation_id: 13,
        client_id: "kayrock",
        groups: [
          %{group_id: "g1", topics: [%{name: "t", partition_indexes: [0]}]},
          %{group_id: "g2", topics: nil}
        ],
        require_stable: false
      }

      expected =
        request_header(8) <>
          <<
            # groups: 2 elements
            3,
            3,
            "g1",
            2,
            2,
            "t",
            2,
            0::32,
            0,
            0,
            3,
            "g2",
            # topics null
            0,
            0,
            # require_stable, tag buffer
            0,
            0
          >>

      assert serialize(request) == expected
    end

    test "V9 adds member_id and member_epoch per group" do
      request = %OffsetFetch.V9.Request{
        correlation_id: 13,
        client_id: "kayrock",
        groups: [
          %{
            group_id: "g1",
            member_id: "mid",
            member_epoch: 4,
            topics: [%{name: "t", partition_indexes: [0]}]
          }
        ],
        require_stable: true
      }

      expected =
        request_header(9) <>
          <<2, 3, "g1", 4, "mid", 4::32, 2, 2, "t", 2, 0::32, 0, 0, 1, 0>>

      assert serialize(request) == expected
    end

    test "V10 identifies topics by id (null member_id, epoch -1 outside a group)" do
      request = %OffsetFetch.V10.Request{
        correlation_id: 13,
        client_id: "kayrock",
        groups: [
          %{
            group_id: "g1",
            member_id: nil,
            member_epoch: -1,
            topics: [%{topic_id: @topic_id, partition_indexes: [0]}]
          }
        ],
        require_stable: false
      }

      expected =
        request_header(10) <>
          <<2, 3, "g1", 0, -1::32-signed, 2>> <> @topic_id <> <<2, 0::32, 0, 0, 0, 0>>

      assert serialize(request) == expected
    end
  end

  describe "responses" do
    test "V7 (single group layout)" do
      data = <<13::32, 0, 0::32, 2, 2, "t">> <> @partition_bytes <> <<0::16, 0>>

      assert {%OffsetFetch.V7.Response{
                correlation_id: 13,
                throttle_time_ms: 0,
                topics: [%{name: "t", partitions: [@partition], tagged_fields: []}],
                error_code: 0,
                tagged_fields: []
              }, <<>>} = OffsetFetch.deserialize(7, data)
    end

    for vsn <- [8, 9] do
      test "V#{vsn} (groups, topic names)" do
        vsn = unquote(vsn)

        data =
          <<13::32, 0, 0::32, 2, 3, "g1", 2, 2, "t">> <>
            @partition_bytes <>
            <<
              # group error_code, group tag buffer, response tag buffer
              0::16,
              0,
              0
            >>

        assert {response, <<>>} = OffsetFetch.deserialize(vsn, data)

        assert response.groups == [
                 %{
                   group_id: "g1",
                   topics: [%{name: "t", partitions: [@partition], tagged_fields: []}],
                   error_code: 0,
                   tagged_fields: []
                 }
               ]
      end
    end

    test "V10 (groups, topic ids)" do
      data =
        <<13::32, 0, 0::32, 2, 3, "g1", 2>> <> @topic_id <> @partition_bytes <> <<0::16, 0, 0>>

      assert {%OffsetFetch.V10.Response{groups: [group]}, <<>>} =
               OffsetFetch.V10.Response.deserialize(data)

      assert group.topics == [%{topic_id: @topic_id, partitions: [@partition], tagged_fields: []}]
    end

    test "V10 group-level STALE_MEMBER_EPOCH" do
      data = <<13::32, 0, 0::32, 2, 3, "g1", 1, 113::16, 0, 0>>

      assert {%{groups: [%{group_id: "g1", topics: [], error_code: 113}]}, <<>>} =
               OffsetFetch.V10.Response.deserialize(data)
    end
  end
end
