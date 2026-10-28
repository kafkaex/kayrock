defmodule Kayrock.Generated.DeleteRecordsFlexibleTest do
  @moduledoc """
  DeleteRecords v2 (JSON-derived, flexible): byte-level fixtures
  hand-derived from `DeleteRecordsRequest.json` / `DeleteRecordsResponse.json`
  (Kafka 4.4.0). v2 is v1 with flexible encodings.
  """
  use ExUnit.Case, async: true

  alias Kayrock.DeleteRecords.V2.Request
  alias Kayrock.DeleteRecords.V2.Response

  test "the generated range now reaches v2" do
    assert Kayrock.DeleteRecords.max_vsn() == 2
    assert Kayrock.KafkaSchemaMetadata.version_range(:delete_records) == {0, 2}
  end

  test "v2 request" do
    request = %Request{
      correlation_id: 7,
      client_id: "kr",
      topics: [
        %{
          name: "t",
          partitions: [%{partition_index: 0, offset: 5}, %{partition_index: 1, offset: -1}]
        }
      ],
      timeout_ms: 3000
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             IO.iodata_to_binary([
               # header v2
               <<21::16, 2::16, 7::32, 2::16, "kr", 0>>,
               # topics: 1 {name "t", partitions: 2}
               <<2, 2, "t", 3>>,
               <<0::32, 5::64, 0>>,
               <<1::32, -1::64-signed, 0>>,
               # topic tags, timeout_ms, root tags
               <<0, 3000::32, 0>>
             ])
  end

  test "v2 response" do
    wire =
      IO.iodata_to_binary([
        # header v1, throttle_time_ms 0
        <<7::32, 0, 0::32>>,
        # topics: 1 {name "t", partitions: 2}
        <<2, 2, "t", 3>>,
        # partition 0: low_watermark 5, error_code 0
        <<0::32, 5::64, 0::16, 0>>,
        # partition 1: low_watermark -1, error_code 1 (OFFSET_OUT_OF_RANGE)
        <<1::32, -1::64-signed, 1::16, 0>>,
        # topic tags, root tags
        <<0, 0>>
      ])

    assert {response, <<>>} = Response.deserialize(wire)

    assert response == %Response{
             correlation_id: 7,
             throttle_time_ms: 0,
             topics: [
               %{
                 name: "t",
                 partitions: [
                   %{partition_index: 0, low_watermark: 5, error_code: 0, tagged_fields: []},
                   %{partition_index: 1, low_watermark: -1, error_code: 1, tagged_fields: []}
                 ],
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }
  end
end
