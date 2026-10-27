defmodule Kayrock.ConsumerGroupDescribeTest do
  @moduledoc """
  ConsumerGroupDescribe (API key 69, KIP-848) v0-v1.

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `ConsumerGroupDescribeRequest.json` / `ConsumerGroupDescribeResponse.json`
  specs. All versions are flexible. `Assignment` and `TargetAssignment` are
  (non-nullable) structs sharing the `Assignment` / `TopicPartitions` common
  structs; v1 adds `MemberType` (int8) at the end of each member.
  """
  use ExUnit.Case, async: true

  alias Kayrock.ConsumerGroupDescribe

  @topic_id <<0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x47, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
              0xFF, 0x00>>

  defp request_header(vsn), do: <<69::16, vsn::16, 9::32, 7::16, "kayrock", 0>>

  # One group with one member; `member_type` is appended (v1) when given.
  defp response_binary(member_type) do
    member_type_bytes = if member_type, do: <<member_type::8-signed>>, else: <<>>

    IO.iodata_to_binary([
      # correlation id + header tag buffer
      <<9::32, 0>>,
      # throttle_time_ms
      <<0::32>>,
      # groups: 1 element
      <<2>>,
      # error_code, error_message null
      <<0::16, 0>>,
      # group_id "g1", group_state "Stable"
      <<3, "g1", 7, "Stable">>,
      # group_epoch, assignment_epoch
      <<4::32, 4::32>>,
      # assignor_name "uniform"
      <<8, "uniform">>,
      # members: 1 element
      <<2>>,
      # member_id "mid", instance_id null, rack_id null
      <<4, "mid", 0, 0>>,
      # member_epoch
      <<4::32>>,
      # client_id "kayrock", client_host "/10.0.0.1"
      <<8, "kayrock", 10, "/10.0.0.1">>,
      # subscribed_topic_names ["t"], subscribed_topic_regex null
      <<2, 2, "t", 0>>,
      # assignment: topic_partitions [{id, "t", [0, 1]}]
      <<2>>,
      @topic_id,
      <<2, "t", 3, 0::32, 1::32, 0>>,
      # assignment tag buffer
      <<0>>,
      # target_assignment: topic_partitions [] + tag buffer
      <<1, 0>>,
      member_type_bytes,
      # member tag buffer
      <<0>>,
      # authorized_operations (not requested)
      <<-2_147_483_648::32-signed>>,
      # group tag buffer, response tag buffer
      <<0, 0>>
    ])
  end

  defp expected_member do
    %{
      member_id: "mid",
      instance_id: nil,
      rack_id: nil,
      member_epoch: 4,
      client_id: "kayrock",
      client_host: "/10.0.0.1",
      subscribed_topic_names: ["t"],
      subscribed_topic_regex: nil,
      assignment: %{
        topic_partitions: [
          %{topic_id: @topic_id, topic_name: "t", partitions: [0, 1], tagged_fields: []}
        ],
        tagged_fields: []
      },
      target_assignment: %{topic_partitions: [], tagged_fields: []},
      tagged_fields: []
    }
  end

  defp expected_group(member) do
    %{
      error_code: 0,
      error_message: nil,
      group_id: "g1",
      group_state: "Stable",
      group_epoch: 4,
      assignment_epoch: 4,
      assignor_name: "uniform",
      members: [member],
      authorized_operations: -2_147_483_648,
      tagged_fields: []
    }
  end

  test "version range and dispatch" do
    assert ConsumerGroupDescribe.min_vsn() == 0
    assert ConsumerGroupDescribe.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:consumer_group_describe) == 69

    for vsn <- 0..1 do
      assert Kayrock.Request.api_vsn(ConsumerGroupDescribe.get_request_struct(vsn)) == vsn
    end
  end

  for vsn <- 0..1 do
    test "V#{vsn} serializes a request" do
      vsn = unquote(vsn)

      request = %{
        ConsumerGroupDescribe.get_request_struct(vsn)
        | correlation_id: 9,
          client_id: "kayrock",
          group_ids: ["g1", "g2"],
          include_authorized_operations: true
      }

      # group_ids ["g1", "g2"], include_authorized_operations true, tag buffer
      expected = request_header(vsn) <> <<3, 3, "g1", 3, "g2", 1, 0>>
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end
  end

  test "V0 deserializes a response" do
    assert {response, <<>>} = ConsumerGroupDescribe.V0.Response.deserialize(response_binary(nil))

    assert response == %ConsumerGroupDescribe.V0.Response{
             correlation_id: 9,
             throttle_time_ms: 0,
             groups: [expected_group(expected_member())],
             tagged_fields: []
           }
  end

  test "V1 deserializes a response with member_type" do
    # member_type 1 = consumer (KIP-848) member
    assert {response, <<>>} = ConsumerGroupDescribe.deserialize(1, response_binary(1))

    assert response == %ConsumerGroupDescribe.V1.Response{
             correlation_id: 9,
             throttle_time_ms: 0,
             groups: [expected_group(Map.put(expected_member(), :member_type, 1))],
             tagged_fields: []
           }
  end

  test "V1 deserializes a GROUP_ID_NOT_FOUND group" do
    data =
      IO.iodata_to_binary([
        <<9::32, 0, 0::32, 2>>,
        # error_code 69 (GROUP_ID_NOT_FOUND), error_message "nope"
        <<69::16, 5, "nope">>,
        <<3, "gx", 1>>,
        <<0::32, 0::32, 1>>,
        # members []
        <<1>>,
        <<-2_147_483_648::32-signed, 0, 0>>
      ])

    assert {%{groups: [%{error_code: 69, error_message: "nope", group_id: "gx", members: []}]},
            <<>>} = ConsumerGroupDescribe.V1.Response.deserialize(data)
  end
end
