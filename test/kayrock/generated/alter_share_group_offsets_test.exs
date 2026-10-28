defmodule Kayrock.AlterShareGroupOffsetsTest do
  @moduledoc """
  AlterShareGroupOffsets (API key 91, KIP-932), v0. Always flexible.

  Fixtures are derived by hand from
  `AlterShareGroupOffsets{Request,Response}.json` (Kafka 4.4.0).
  """
  use ExUnit.Case, async: true

  alias Kayrock.AlterShareGroupOffsets
  alias Kayrock.AlterShareGroupOffsets.V0.{Request, Response}

  @topic_id <<0x99::128>>

  test "version range" do
    assert AlterShareGroupOffsets.min_vsn()..AlterShareGroupOffsets.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:alter_share_group_offsets)

    assert AlterShareGroupOffsets.min_vsn() == 0
    assert AlterShareGroupOffsets.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:alter_share_group_offsets) == 91
    assert %Request{} = AlterShareGroupOffsets.get_request_struct(0)
  end

  test "v0 request" do
    request = %Request{
      correlation_id: 2,
      client_id: "c",
      group_id: "g",
      topics: [
        %{
          topic_name: "t",
          partitions: [
            %{partition_index: 0, start_offset: 10},
            %{partition_index: 1, start_offset: -1}
          ]
        }
      ]
    }

    expected = <<
      # header v2
      91::16,
      0::16,
      2::32,
      1::16,
      "c",
      0,
      # group_id
      2,
      "g",
      # topics: 1
      2,
      2,
      "t",
      # partitions: 2 {partition_index, start_offset, tags}
      3,
      0::32,
      10::64,
      0,
      1::32,
      -1::64-signed,
      0,
      # topic tags, tagged fields
      0,
      0
    >>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v0 response" do
    data = <<
      # correlation_id, header tagged fields
      2::32,
      0,
      # throttle_time_ms, error_code, error_message null
      0::32,
      0::16,
      0,
      # responses: 1
      2,
      2,
      "t",
      @topic_id::binary,
      # partitions: 2
      3,
      0::32,
      0::16,
      0,
      0,
      # UNKNOWN_TOPIC_OR_PARTITION, error_message "p"
      1::32,
      3::16,
      2,
      "p",
      0,
      # topic tags, tagged fields
      0,
      0
    >>

    assert AlterShareGroupOffsets.deserialize(0, data) ==
             {%Response{
                correlation_id: 2,
                throttle_time_ms: 0,
                error_code: 0,
                error_message: nil,
                responses: [
                  %{
                    topic_name: "t",
                    topic_id: @topic_id,
                    partitions: [
                      %{partition_index: 0, error_code: 0, error_message: nil, tagged_fields: []},
                      %{partition_index: 1, error_code: 3, error_message: "p", tagged_fields: []}
                    ],
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end

  test "v0 top-level error (NON_EMPTY_GROUP)" do
    data = <<2::32, 0, 0::32, 68::16, 2, "n", 1, 0>>

    assert {%Response{error_code: 68, error_message: "n", responses: []}, <<>>} =
             Response.deserialize(data)
  end
end
