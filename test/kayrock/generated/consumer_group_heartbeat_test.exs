defmodule Kayrock.ConsumerGroupHeartbeatTest do
  @moduledoc """
  ConsumerGroupHeartbeat (API key 68, KIP-848) v0-v1.

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `ConsumerGroupHeartbeatRequest.json` / `ConsumerGroupHeartbeatResponse.json`
  specs. Every version is flexible: request header v2 (client id + empty tag
  buffer), response header v1 (correlation id + tag buffer), compact strings /
  arrays (length + 1), and the nullable `Assignment` struct is an int8
  presence marker (-1 = null, 1 = present).
  """
  use ExUnit.Case, async: true

  alias Kayrock.ConsumerGroupHeartbeat

  @topic_id <<0x5B, 0x1F, 0x43, 0xA7, 0x10, 0x2C, 0x4E, 0x89, 0x9A, 0x01, 0x6D, 0x3E, 0x77, 0xC2,
              0x08, 0xF4>>

  # request header v2 for api key 68 (correlation id 3, client id "kayrock")
  defp request_header(vsn), do: <<68::16, vsn::16, 3::32, 7::16, "kayrock", 0>>

  test "version range and dispatch" do
    assert ConsumerGroupHeartbeat.min_vsn() == 0
    assert ConsumerGroupHeartbeat.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:consumer_group_heartbeat) == 68

    for vsn <- 0..1 do
      request = ConsumerGroupHeartbeat.get_request_struct(vsn)
      assert Kayrock.Request.api_vsn(request) == vsn
    end
  end

  describe "V0" do
    alias ConsumerGroupHeartbeat.V0.Request
    alias ConsumerGroupHeartbeat.V0.Response

    test "serializes a join (member_epoch 0, server-generated member id)" do
      request = %Request{
        correlation_id: 3,
        client_id: "kayrock",
        group_id: "g1",
        member_id: "",
        member_epoch: 0,
        instance_id: nil,
        rack_id: "r",
        rebalance_timeout_ms: 30_000,
        subscribed_topic_names: ["topic-a"],
        server_assignor: "uniform",
        topic_partitions: []
      }

      expected =
        request_header(0) <>
          <<
            # group_id "g1"
            3,
            "g1",
            # member_id ""
            1,
            # member_epoch 0
            0::32,
            # instance_id null
            0,
            # rack_id "r"
            2,
            "r",
            # rebalance_timeout_ms
            30_000::32,
            # subscribed_topic_names ["topic-a"]
            2,
            8,
            "topic-a",
            # server_assignor "uniform"
            8,
            "uniform",
            # topic_partitions [] (empty, not null)
            1,
            # tag buffer
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "deserializes a response with an assignment" do
      data =
        <<3::32, 0>> <>
          <<
            # throttle_time_ms
            0::32,
            # error_code
            0::16,
            # error_message null
            0,
            # member_id "m-1"
            4,
            "m-1",
            # member_epoch
            1::32,
            # heartbeat_interval_ms
            5000::32,
            # assignment present
            1,
            # topic_partitions: 1 element
            2
          >> <>
          @topic_id <>
          <<
            # partitions [0, 1, 2]
            4,
            0::32,
            1::32,
            2::32,
            # topic partition, assignment and response tag buffers
            0,
            0,
            0
          >>

      assert {response, <<>>} = Response.deserialize(data)

      assert response == %Response{
               correlation_id: 3,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               member_id: "m-1",
               member_epoch: 1,
               heartbeat_interval_ms: 5000,
               assignment: %{
                 topic_partitions: [
                   %{topic_id: @topic_id, partitions: [0, 1, 2], tagged_fields: []}
                 ],
                 tagged_fields: []
               },
               tagged_fields: []
             }

      assert ConsumerGroupHeartbeat.deserialize(0, data) == {response, <<>>}
    end

    test "deserializes an error response with a null assignment" do
      # FENCED_MEMBER_EPOCH (110)
      data =
        <<3::32, 0, 0::32, 110::16, 7, "fenced", 0, -1::32-signed, 0::32, -1::8-signed, 0>>

      assert {%Response{
                error_code: 110,
                error_message: "fenced",
                member_id: nil,
                member_epoch: -1,
                assignment: nil
              }, <<>>} = Response.deserialize(data)
    end
  end

  describe "V1" do
    alias ConsumerGroupHeartbeat.V1.Request
    alias ConsumerGroupHeartbeat.V1.Response

    test "serializes a join with a client-generated member id and a topic regex" do
      request = %Request{
        correlation_id: 3,
        client_id: "kayrock",
        group_id: "g1",
        member_id: "mid",
        member_epoch: 0,
        instance_id: "i1",
        rack_id: nil,
        rebalance_timeout_ms: 45_000,
        subscribed_topic_names: nil,
        subscribed_topic_regex: "topic-.*",
        server_assignor: nil,
        topic_partitions: []
      }

      expected =
        request_header(1) <>
          <<
            3,
            "g1",
            # member_id "mid"
            4,
            "mid",
            0::32,
            # instance_id "i1"
            3,
            "i1",
            # rack_id null
            0,
            45_000::32,
            # subscribed_topic_names null
            0,
            # subscribed_topic_regex "topic-.*" (v1+)
            9,
            "topic-.*",
            # server_assignor null
            0,
            # topic_partitions []
            1,
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "serializes a heartbeat reporting owned partitions" do
      request = %Request{
        correlation_id: 3,
        client_id: "kayrock",
        group_id: "g1",
        member_id: "mid",
        member_epoch: 7,
        rebalance_timeout_ms: -1,
        subscribed_topic_names: nil,
        topic_partitions: [%{topic_id: @topic_id, partitions: [2, 0], tagged_fields: []}]
      }

      expected =
        request_header(1) <>
          <<3, "g1", 4, "mid", 7::32>> <>
          <<
            # instance_id, rack_id null
            0,
            0,
            -1::32-signed,
            # subscribed_topic_names, subscribed_topic_regex, server_assignor null
            0,
            0,
            0,
            # topic_partitions: 1 element
            2
          >> <>
          @topic_id <> <<3, 2::32, 0::32, 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "serializes a leave (member_epoch -1, everything else unchanged)" do
      request = %Request{
        correlation_id: 3,
        client_id: "kayrock",
        group_id: "g1",
        member_id: "mid",
        member_epoch: -1,
        rebalance_timeout_ms: -1,
        subscribed_topic_names: nil,
        topic_partitions: nil
      }

      expected =
        request_header(1) <>
          <<3, "g1", 4, "mid", -1::32-signed, 0, 0, -1::32-signed, 0, 0, 0, 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "deserializes a response with an empty assignment" do
      data = <<3::32, 0, 0::32, 0::16, 0, 4, "mid", -1::32-signed, 5000::32, 1, 1, 0, 0>>

      assert {%Response{
                member_id: "mid",
                member_epoch: -1,
                heartbeat_interval_ms: 5000,
                assignment: %{topic_partitions: [], tagged_fields: []},
                tagged_fields: []
              }, <<>>} = Response.deserialize(data)

      assert {%Response{}, <<>>} = ConsumerGroupHeartbeat.deserialize(1, data)
    end
  end
end
