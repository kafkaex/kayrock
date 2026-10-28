defmodule Kayrock.Generated.OffsetForLeaderEpochFlexibleTest do
  @moduledoc """
  OffsetForLeaderEpoch v4 (JSON-derived, flexible): byte-level fixtures
  hand-derived from `OffsetForLeaderEpochRequest.json` /
  `OffsetForLeaderEpochResponse.json` (Kafka 4.4.0). v4 is v3 with flexible
  encodings.
  """
  use ExUnit.Case, async: true

  alias Kayrock.OffsetForLeaderEpoch.V4.Request
  alias Kayrock.OffsetForLeaderEpoch.V4.Response

  test "the generated range now reaches v4" do
    assert Kayrock.OffsetForLeaderEpoch.max_vsn() == 4
    assert Kayrock.KafkaSchemaMetadata.version_range(:offset_for_leader_epoch) == {0, 4}
  end

  test "v4 request" do
    request = %Request{
      correlation_id: 7,
      client_id: "kr",
      replica_id: -1,
      topics: [
        %{
          topic: "t",
          partitions: [%{partition: 0, current_leader_epoch: -1, leader_epoch: 0}]
        }
      ]
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             IO.iodata_to_binary([
               # header v2
               <<23::16, 4::16, 7::32, 2::16, "kr", 0>>,
               # replica_id -1
               <<-1::32-signed>>,
               # topics: 1 {topic "t", partitions: 1 {partition 0,
               # current_leader_epoch -1, leader_epoch 0, tags}, tags}
               <<2, 2, "t", 2, 0::32, -1::32-signed, 0::32, 0, 0>>,
               # root tags
               <<0>>
             ])
  end

  test "v4 response" do
    wire =
      IO.iodata_to_binary([
        # header v1, throttle_time_ms 0
        <<7::32, 0, 0::32>>,
        # topics: 1 {topic "t", partitions: 1}
        <<2, 2, "t", 2>>,
        # error_code 0, partition 0, leader_epoch 0, end_offset 3, tags
        <<0::16, 0::32, 0::32, 3::64, 0>>,
        # topic tags, root tags
        <<0, 0>>
      ])

    assert {response, <<>>} = Response.deserialize(wire)

    assert response == %Response{
             correlation_id: 7,
             throttle_time_ms: 0,
             topics: [
               %{
                 topic: "t",
                 partitions: [
                   %{
                     error_code: 0,
                     partition: 0,
                     leader_epoch: 0,
                     end_offset: 3,
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }
  end
end
