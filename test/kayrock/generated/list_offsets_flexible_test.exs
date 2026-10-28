defmodule Kayrock.Generated.ListOffsetsFlexibleTest do
  @moduledoc """
  ListOffsets v6-v11 (JSON-derived, flexible): byte-level fixtures
  hand-derived from `ListOffsetsRequest.json` / `ListOffsetsResponse.json`
  (Kafka 4.4.0).

  v6 enables flexible versions; v7-v9 only add new special timestamps
  (-3 max timestamp, -4 local log start, -5 last tiered offset); v10 adds
  `TimeoutMs`; v11 adds the -6 earliest pending upload timestamp. The wire
  shapes are therefore v6 = v7 = v8 = v9 and v10 = v11.
  """
  use ExUnit.Case, async: true

  defp request_module(vsn), do: Module.concat([Kayrock.ListOffsets, :"V#{vsn}", Request])
  defp response_module(vsn), do: Module.concat([Kayrock.ListOffsets, :"V#{vsn}", Response])

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  test "the generated range now reaches v11" do
    assert Kayrock.ListOffsets.max_vsn() == 11
    assert Kayrock.KafkaSchemaMetadata.version_range(:list_offsets) == {0, 11}
  end

  for vsn <- 6..11 do
    test "v#{vsn} request" do
      vsn = unquote(vsn)

      request =
        struct(request_module(vsn),
          correlation_id: 7,
          client_id: "kr",
          replica_id: -1,
          isolation_level: 1,
          topics: [
            %{
              name: "t",
              partitions: [
                %{partition_index: 0, current_leader_epoch: -1, timestamp: -1},
                %{partition_index: 1, current_leader_epoch: 3, timestamp: -2}
              ]
            }
          ],
          timeout_ms: 2000
        )

      expected =
        IO.iodata_to_binary([
          # header v2
          <<2::16, vsn::16, 7::32, 2::16, "kr", 0>>,
          # replica_id -1, isolation_level 1
          <<-1::32-signed, 1>>,
          # topics: 1 {name "t", partitions: 2}
          <<2, 2, "t", 3>>,
          <<0::32, -1::32-signed, -1::64-signed, 0>>,
          <<1::32, 3::32, -2::64-signed, 0>>,
          # topic tags
          <<0>>,
          # timeout_ms (v10+)
          if(vsn >= 10, do: <<2000::32>>, else: <<>>),
          # root tags
          <<0>>
        ])

      assert serialize(request) == expected
    end

    test "v#{vsn} response" do
      vsn = unquote(vsn)

      wire =
        IO.iodata_to_binary([
          # header v1
          <<7::32, 0>>,
          # throttle_time_ms 0
          <<0::32>>,
          # topics: 1 {name "t", partitions: 1 {partition_index 0, error_code 0,
          # timestamp -1, offset 12, leader_epoch 0, tags}, tags}
          <<2, 2, "t", 2, 0::32, 0::16, -1::64-signed, 12::64, 0::32, 0, 0>>,
          # root tags
          <<0>>
        ])

      assert {response, <<>>} = response_module(vsn).deserialize(wire)
      assert response.correlation_id == 7
      assert response.throttle_time_ms == 0

      assert response.topics == [
               %{
                 name: "t",
                 partitions: [
                   %{
                     partition_index: 0,
                     error_code: 0,
                     timestamp: -1,
                     offset: 12,
                     leader_epoch: 0,
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ]

      assert response.tagged_fields == []
    end
  end

  test "unknown tags are preserved on decode" do
    wire = <<7::32, 0, 0::32, 1, 1, 9, 2, "ab">>
    assert {response, <<>>} = response_module(11).deserialize(wire)
    assert response.topics == []
    assert response.tagged_fields == [{9, "ab"}]
  end
end
