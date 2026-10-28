defmodule Kayrock.ShareGroupDescribeTest do
  @moduledoc """
  ShareGroupDescribe (API key 77, KIP-932), v1 only (v0 was early access in
  Kafka 4.0 and removed in 4.1). Always flexible.

  Fixtures are derived by hand from `ShareGroupDescribe{Request,Response}.json`
  (Kafka 4.4.0); `share_group_describe_v1_response.bin` was captured from an
  `apache/kafka:4.4.0` broker.
  """
  use ExUnit.Case, async: true

  alias Kayrock.ShareGroupDescribe
  alias Kayrock.ShareGroupDescribe.V1.{Request, Response}

  @topic_id <<0xAB::128>>
  @fixtures Path.expand("../../fixtures/share_groups", __DIR__)

  test "version range" do
    assert ShareGroupDescribe.min_vsn()..ShareGroupDescribe.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:share_group_describe)

    assert ShareGroupDescribe.min_vsn() == 1
    assert ShareGroupDescribe.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:share_group_describe) == 77
    assert %Request{} = ShareGroupDescribe.get_request_struct(1)
  end

  test "v1 request" do
    request = %Request{
      correlation_id: 3,
      client_id: "c",
      group_ids: ["g1", "g2"],
      include_authorized_operations: true
    }

    expected = <<
      # header v2
      77::16,
      1::16,
      3::32,
      1::16,
      "c",
      0,
      # group_ids: 2 compact strings
      3,
      3,
      "g1",
      3,
      "g2",
      # include_authorized_operations
      1,
      # tagged fields
      0
    >>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v1 response with one member" do
    data = <<
      # correlation_id, header tagged fields
      3::32,
      0,
      # throttle_time_ms
      0::32,
      # groups: 1
      2,
      # error_code, error_message null
      0::16,
      0,
      # group_id "g1", group_state "Stable"
      3,
      "g1",
      7,
      "Stable",
      # group_epoch, assignment_epoch
      2::32,
      2::32,
      # assignor_name "simple"
      7,
      "simple",
      # members: 1
      2,
      # member_id "m", rack_id null, member_epoch
      2,
      "m",
      0,
      2::32,
      # client_id "cl", client_host "/h"
      3,
      "cl",
      3,
      "/h",
      # subscribed_topic_names ["t"]
      2,
      2,
      "t",
      # assignment.topic_partitions: 1
      2,
      @topic_id::binary,
      2,
      "t",
      # partitions [0]
      2,
      0::32,
      # topic_partitions[0] tagged fields, assignment tagged fields
      0,
      0,
      # member tagged fields
      0,
      # authorized_operations
      3448::32,
      # group tagged fields
      0,
      # tagged fields
      0
    >>

    expected = %Response{
      correlation_id: 3,
      throttle_time_ms: 0,
      groups: [
        %{
          error_code: 0,
          error_message: nil,
          group_id: "g1",
          group_state: "Stable",
          group_epoch: 2,
          assignment_epoch: 2,
          assignor_name: "simple",
          members: [
            %{
              member_id: "m",
              rack_id: nil,
              member_epoch: 2,
              client_id: "cl",
              client_host: "/h",
              subscribed_topic_names: ["t"],
              assignment: %{
                topic_partitions: [
                  %{topic_id: @topic_id, topic_name: "t", partitions: [0], tagged_fields: []}
                ],
                tagged_fields: []
              },
              tagged_fields: []
            }
          ],
          authorized_operations: 3448,
          tagged_fields: []
        }
      ],
      tagged_fields: []
    }

    assert Response.deserialize(data) == {expected, <<>>}
    assert ShareGroupDescribe.deserialize(1, data) == {expected, <<>>}
  end

  test "v1 response for an unknown group" do
    data = <<
      1::32,
      0,
      0::32,
      2,
      # GROUP_ID_NOT_FOUND, error_message "no"
      69::16,
      3,
      "no",
      2,
      "g",
      # empty group_state / assignor_name, epochs 0, members []
      1,
      0::32,
      0::32,
      1,
      1,
      # authorized_operations INT32_MIN (not requested)
      -2_147_483_648::32-signed,
      0,
      0
    >>

    assert {%Response{
              groups: [
                %{
                  error_code: 69,
                  error_message: "no",
                  group_id: "g",
                  group_state: "",
                  members: [],
                  authorized_operations: -2_147_483_648
                }
              ]
            }, <<>>} = Response.deserialize(data)
  end

  test "v1 response captured from apache/kafka:4.4.0" do
    data = File.read!(Path.join(@fixtures, "share_group_describe_v1_response.bin"))

    assert {%Response{groups: [group]}, <<>>} = Response.deserialize(data)
    assert group.error_code == 0
    assert group.group_state == "Stable"
    assert group.assignor_name == "simple"
    assert [member] = group.members
    assert member.client_id == "kayrock-share-groups"
    assert [topic] = member.subscribed_topic_names

    assert [%{topic_name: ^topic, topic_id: <<_::128>>, partitions: [0]}] =
             member.assignment.topic_partitions
  end
end
