defmodule Kayrock.DeleteShareGroupOffsetsTest do
  @moduledoc """
  DeleteShareGroupOffsets (API key 92, KIP-932), v0. Always flexible.

  Fixtures are derived by hand from
  `DeleteShareGroupOffsets{Request,Response}.json` (Kafka 4.4.0).
  """
  use ExUnit.Case, async: true

  alias Kayrock.DeleteShareGroupOffsets
  alias Kayrock.DeleteShareGroupOffsets.V0.{Request, Response}

  @topic_id <<0x77::128>>

  test "version range" do
    assert DeleteShareGroupOffsets.min_vsn()..DeleteShareGroupOffsets.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:delete_share_group_offsets)

    assert DeleteShareGroupOffsets.min_vsn() == 0
    assert DeleteShareGroupOffsets.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:delete_share_group_offsets) == 92
    assert %Request{} = DeleteShareGroupOffsets.get_request_struct(0)
  end

  test "v0 request" do
    request = %Request{
      correlation_id: 8,
      client_id: "c",
      group_id: "g",
      topics: [%{topic_name: "t1"}, %{topic_name: "t2"}]
    }

    expected = <<
      # header v2
      92::16,
      0::16,
      8::32,
      1::16,
      "c",
      0,
      # group_id
      2,
      "g",
      # topics: 2 {topic_name, tags}
      3,
      3,
      "t1",
      0,
      3,
      "t2",
      0,
      # tagged fields
      0
    >>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v0 response" do
    data = <<
      # correlation_id, header tagged fields
      8::32,
      0,
      # throttle_time_ms, error_code, error_message null
      0::32,
      0::16,
      0,
      # responses: 2 {topic_name, topic_id, error_code, error_message, tags}
      3,
      3,
      "t1",
      @topic_id::binary,
      0::16,
      0,
      0,
      3,
      "t2",
      # unknown topic: zero topic id, UNKNOWN_TOPIC_OR_PARTITION
      0::128,
      3::16,
      2,
      "u",
      0,
      # tagged fields
      0
    >>

    assert DeleteShareGroupOffsets.deserialize(0, data) ==
             {%Response{
                correlation_id: 8,
                throttle_time_ms: 0,
                error_code: 0,
                error_message: nil,
                responses: [
                  %{
                    topic_name: "t1",
                    topic_id: @topic_id,
                    error_code: 0,
                    error_message: nil,
                    tagged_fields: []
                  },
                  %{
                    topic_name: "t2",
                    topic_id: <<0::128>>,
                    error_code: 3,
                    error_message: "u",
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end

  test "v0 top-level error (GROUP_ID_NOT_FOUND)" do
    data = <<8::32, 0, 0::32, 69::16, 0, 1, 0>>

    assert {%Response{error_code: 69, error_message: nil, responses: []}, <<>>} =
             Response.deserialize(data)
  end
end
