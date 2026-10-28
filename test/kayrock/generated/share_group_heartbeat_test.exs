defmodule Kayrock.ShareGroupHeartbeatTest do
  @moduledoc """
  ShareGroupHeartbeat (API key 76, KIP-932), v1 only (v0 was early access in
  Kafka 4.0 and removed in 4.1). Always flexible: request header v2,
  response header v1.

  Fixtures are derived by hand from `ShareGroupHeartbeat{Request,Response}.json`
  (Kafka 4.4.0); `share_group_heartbeat_v1_response.bin` was captured from an
  `apache/kafka:4.4.0` broker.
  """
  use ExUnit.Case, async: true

  alias Kayrock.ShareGroupHeartbeat
  alias Kayrock.ShareGroupHeartbeat.V1.{Request, Response}

  @topic_id <<1::128>>
  @fixtures Path.expand("../../fixtures/share_groups", __DIR__)

  test "version range" do
    assert ShareGroupHeartbeat.min_vsn()..ShareGroupHeartbeat.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:share_group_heartbeat)

    assert ShareGroupHeartbeat.min_vsn() == 1
    assert ShareGroupHeartbeat.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:share_group_heartbeat) == 76
    assert Kayrock.KafkaSchemaMetadata.version_range(:share_group_heartbeat) == {1, 1}
    assert %Request{} = ShareGroupHeartbeat.get_request_struct(1)
  end

  describe "v1 request" do
    test "join: subscribed topics, null rack id" do
      request = %Request{
        correlation_id: 7,
        client_id: "kayrock",
        group_id: "g1",
        member_id: "m1",
        member_epoch: 0,
        rack_id: nil,
        subscribed_topic_names: ["t1", "t2"]
      }

      expected = <<
        # header v2: api_key 76, api_version 1, correlation_id 7
        76::16,
        1::16,
        7::32,
        # client_id (int16 length string), header tagged fields
        7::16,
        "kayrock",
        0,
        # group_id compact string "g1"
        3,
        "g1",
        # member_id "m1"
        3,
        "m1",
        # member_epoch 0
        0::32,
        # rack_id null
        0,
        # subscribed_topic_names: 2 entries
        3,
        3,
        "t1",
        3,
        "t2",
        # tagged fields
        0
      >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "steady-state heartbeat: rack id, null topic list, epoch" do
      request = %Request{
        correlation_id: 1,
        client_id: "c",
        group_id: "g",
        member_id: "m",
        member_epoch: 5,
        rack_id: "r",
        subscribed_topic_names: nil
      }

      expected =
        <<76::16, 1::16, 1::32, 1::16, "c", 0, 2, "g", 2, "m", 5::32, 2, "r", 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "leave: epoch -1" do
      request = %Request{
        correlation_id: 1,
        client_id: "c",
        group_id: "g",
        member_id: "m",
        member_epoch: -1,
        subscribed_topic_names: nil
      }

      assert <<76::16, 1::16, 1::32, 1::16, "c", 0, 2, "g", 2, "m", -1::32-signed, 0, 0, 0>> =
               IO.iodata_to_binary(Kayrock.Request.serialize(request))
    end
  end

  describe "v1 response" do
    test "with an assignment" do
      data = <<
        # correlation_id, header tagged fields
        7::32,
        0,
        # throttle_time_ms
        0::32,
        # error_code
        0::16,
        # error_message null
        0,
        # member_id "m1"
        3,
        "m1",
        # member_epoch
        3::32,
        # heartbeat_interval_ms
        5000::32,
        # assignment present
        1,
        # topic_partitions: 1 entry
        2,
        @topic_id::binary,
        # partitions [0, 2]
        3,
        0::32,
        2::32,
        # topic_partitions[0] tagged fields
        0,
        # assignment tagged fields
        0,
        # tagged fields
        0
      >>

      assert {response, <<>>} = Response.deserialize(data)

      assert response == %Response{
               correlation_id: 7,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               member_id: "m1",
               member_epoch: 3,
               heartbeat_interval_ms: 5000,
               assignment: %{
                 topic_partitions: [
                   %{topic_id: @topic_id, partitions: [0, 2], tagged_fields: []}
                 ],
                 tagged_fields: []
               },
               tagged_fields: []
             }

      assert ShareGroupHeartbeat.deserialize(1, data) == {response, <<>>}
    end

    test "null assignment and an error" do
      data = <<
        1::32,
        0,
        0::32,
        # NOT_COORDINATOR
        16::16,
        # error_message "x"
        2,
        "x",
        # member_id null
        0,
        0::32,
        0::32,
        # assignment null
        -1::8-signed,
        0
      >>

      assert {%Response{
                error_code: 16,
                error_message: "x",
                member_id: nil,
                assignment: nil,
                tagged_fields: []
              }, <<>>} = Response.deserialize(data)
    end

    test "captured from apache/kafka:4.4.0" do
      data = File.read!(Path.join(@fixtures, "share_group_heartbeat_v1_response.bin"))

      assert {response, <<>>} = Response.deserialize(data)
      assert response.error_code == 0
      assert response.member_epoch == 3
      assert response.heartbeat_interval_ms == 5000
      assert byte_size(response.member_id) == 22

      assert %{topic_partitions: [%{topic_id: <<_::128>>, partitions: [0]}]} =
               response.assignment
    end
  end
end
