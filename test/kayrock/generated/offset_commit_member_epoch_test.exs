defmodule Kayrock.OffsetCommitMemberEpochTest do
  @moduledoc """
  OffsetCommit v9-v10 (JSON-sourced; v0-v8 come from `:kpro_schema` and are
  covered by `Kayrock.OffsetCommitTest`).

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `OffsetCommitRequest.json` / `OffsetCommitResponse.json` specs:

    * v9 has the v8 layout; `generation_id_or_member_epoch` carries the
      KIP-848 member epoch when the consumer protocol is used;
    * v10 replaces the topic `name` with `topic_id` (uuid) in request and
      response.
  """
  use ExUnit.Case, async: true

  alias Kayrock.OffsetCommit

  @topic_id <<0x0F, 0x1E, 0x2D, 0x3C, 0x4B, 0x5A, 0x49, 0x78, 0x87, 0x96, 0xA5, 0xB4, 0xC3, 0xD2,
              0xE1, 0xF0>>

  defp request_header(vsn), do: <<8::16, vsn::16, 11::32, 7::16, "kayrock", 0>>

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  @partition %{
    partition_index: 1,
    committed_offset: 42,
    committed_leader_epoch: -1,
    committed_metadata: "md",
    tagged_fields: []
  }

  # group_id "g1", member epoch 3, member_id "mid", group_instance_id null
  @group_prefix <<3, "g1", 3::32, 4, "mid", 0>>

  # partitions: 1 element [{1, 42, -1, "md"}] + topic tag buffer
  @partition_bytes <<2, 1::32, 42::64, -1::32-signed, 3, "md", 0, 0>>

  test "version range and dispatch" do
    assert OffsetCommit.max_vsn() == 10

    for vsn <- 9..10 do
      assert Kayrock.Request.api_vsn(OffsetCommit.get_request_struct(vsn)) == vsn
    end
  end

  test "V9 request carries the member epoch and topic names" do
    request = %OffsetCommit.V9.Request{
      correlation_id: 11,
      client_id: "kayrock",
      group_id: "g1",
      generation_id_or_member_epoch: 3,
      member_id: "mid",
      group_instance_id: nil,
      topics: [%{name: "t", partitions: [@partition], tagged_fields: []}]
    }

    expected =
      request_header(9) <>
        @group_prefix <>
        <<
          # topics: 1 element, name "t"
          2,
          2,
          "t"
        >> <> @partition_bytes <> <<0>>

    assert serialize(request) == expected
  end

  test "V10 request identifies topics by id" do
    request = %OffsetCommit.V10.Request{
      correlation_id: 11,
      client_id: "kayrock",
      group_id: "g1",
      generation_id_or_member_epoch: 3,
      member_id: "mid",
      group_instance_id: nil,
      topics: [%{topic_id: @topic_id, partitions: [@partition], tagged_fields: []}]
    }

    refute Map.has_key?(hd(request.topics), :name)

    expected =
      request_header(10) <> @group_prefix <> <<2>> <> @topic_id <> @partition_bytes <> <<0>>

    assert serialize(request) == expected
  end

  test "V9 response" do
    data = <<11::32, 0, 0::32, 2, 2, "t", 2, 1::32, 0::16, 0, 0, 0>>

    assert {%OffsetCommit.V9.Response{
              correlation_id: 11,
              throttle_time_ms: 0,
              topics: [
                %{
                  name: "t",
                  partitions: [%{partition_index: 1, error_code: 0, tagged_fields: []}],
                  tagged_fields: []
                }
              ],
              tagged_fields: []
            }, <<>>} = OffsetCommit.deserialize(9, data)
  end

  test "V10 response identifies topics by id" do
    # STALE_MEMBER_EPOCH (113)
    data = <<11::32, 0, 0::32, 2>> <> @topic_id <> <<2, 1::32, 113::16, 0, 0, 0>>

    assert {%OffsetCommit.V10.Response{
              topics: [
                %{
                  topic_id: @topic_id,
                  partitions: [%{partition_index: 1, error_code: 113}]
                }
              ]
            }, <<>>} = OffsetCommit.V10.Response.deserialize(data)
  end
end
