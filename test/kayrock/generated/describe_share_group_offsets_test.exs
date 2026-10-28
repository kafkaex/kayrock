defmodule Kayrock.DescribeShareGroupOffsetsTest do
  @moduledoc """
  DescribeShareGroupOffsets (API key 90, KIP-932), v0-v1. Always flexible.

    * v1 response: per-partition Lag (KIP-1226); the request is unchanged

  Fixtures are derived by hand from
  `DescribeShareGroupOffsets{Request,Response}.json` (Kafka 4.4.0).
  """
  use ExUnit.Case, async: true

  alias Kayrock.DescribeShareGroupOffsets

  @topic_id <<0x42::128>>

  test "version range" do
    assert DescribeShareGroupOffsets.min_vsn()..DescribeShareGroupOffsets.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:describe_share_group_offsets)

    assert DescribeShareGroupOffsets.min_vsn() == 0
    assert DescribeShareGroupOffsets.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_share_group_offsets) == 90
  end

  for version <- 0..1 do
    test "v#{version} request with explicit topics and with null (all) topics" do
      version = unquote(version)
      request = DescribeShareGroupOffsets.get_request_struct(version)

      request = %{
        request
        | correlation_id: 5,
          client_id: "c",
          groups: [
            %{group_id: "g1", topics: [%{topic_name: "t", partitions: [0, 1]}]},
            %{group_id: "g2", topics: nil}
          ]
      }

      expected = <<
        # header v2
        90::16,
        version::16,
        5::32,
        1::16,
        "c",
        0,
        # groups: 2
        3,
        3,
        "g1",
        # topics: 1 {topic_name, partitions [0, 1], tags}
        2,
        2,
        "t",
        3,
        0::32,
        1::32,
        0,
        # group tags
        0,
        3,
        "g2",
        # topics null
        0,
        0,
        # tagged fields
        0
      >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end
  end

  defp response_binary(lag, start_offset \\ 6) do
    <<
      # correlation_id, header tagged fields, throttle_time_ms
      5::32,
      0,
      0::32,
      # groups: 1
      2,
      3,
      "g1",
      # topics: 1
      2,
      2,
      "t",
      @topic_id::binary,
      # partitions: 1 {partition_index, start_offset, leader_epoch, [lag]}
      2,
      0::32,
      start_offset::64-signed,
      0::32,
      lag::binary,
      # error_code, error_message null, tags
      0::16,
      0,
      0,
      # topic tags
      0,
      # group error_code, error_message null, tags
      0::16,
      0,
      0,
      # tagged fields
      0
    >>
  end

  test "v0 response" do
    data = response_binary(<<>>)

    assert DescribeShareGroupOffsets.deserialize(0, data) ==
             {%DescribeShareGroupOffsets.V0.Response{
                correlation_id: 5,
                throttle_time_ms: 0,
                groups: [
                  %{
                    group_id: "g1",
                    topics: [
                      %{
                        topic_name: "t",
                        topic_id: @topic_id,
                        partitions: [
                          %{
                            partition_index: 0,
                            start_offset: 6,
                            leader_epoch: 0,
                            error_code: 0,
                            error_message: nil,
                            tagged_fields: []
                          }
                        ],
                        tagged_fields: []
                      }
                    ],
                    error_code: 0,
                    error_message: nil,
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end

  test "v1 response adds lag" do
    data = response_binary(<<3::64>>)

    assert {%DescribeShareGroupOffsets.V1.Response{groups: [group]}, <<>>} =
             DescribeShareGroupOffsets.deserialize(1, data)

    assert [%{topic_id: @topic_id, partitions: [partition]}] = group.topics

    assert partition == %{
             partition_index: 0,
             start_offset: 6,
             leader_epoch: 0,
             lag: 3,
             error_code: 0,
             error_message: nil,
             tagged_fields: []
           }
  end

  test "v1 response for an uninitialized partition (start offset and lag -1)" do
    data = response_binary(<<-1::64-signed>>, -1)

    assert {%{groups: [%{topics: [%{partitions: [partition]}]}]}, <<>>} =
             DescribeShareGroupOffsets.V1.Response.deserialize(data)

    assert %{start_offset: -1, lag: -1} = partition
  end

  test "group-level error" do
    # GROUP_ID_NOT_FOUND with no topics
    data = <<5::32, 0, 0::32, 2, 2, "g", 1, 69::16, 2, "x", 0, 0>>

    assert {%{groups: [%{group_id: "g", topics: [], error_code: 69, error_message: "x"}]}, <<>>} =
             DescribeShareGroupOffsets.deserialize(0, data)
  end
end
