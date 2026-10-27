defmodule Kayrock.TxnOffsetCommitJsonVersionsTest do
  @moduledoc """
  Wire fixtures for the JSON-derived TxnOffsetCommit versions (v3-v6),
  derived by hand from
  `priv/kafka_messages/4.4.0/TxnOffsetCommit{Request,Response}.json`.

  v3 is the first flexible version and adds GenerationIdOrMemberEpoch,
  MemberId and GroupInstanceId; v4/v5 add error codes only; v6 (KIP-1319)
  identifies topics by `topic_id` (uuid) instead of `name`.
  """
  use ExUnit.Case, async: true

  @api_key 28
  @topic_id <<0x0123456789ABCDEF0011223344556677::128>>

  test "generated range" do
    assert Kayrock.KafkaSchemaMetadata.version_range(:txn_offset_commit) == {0, 6}
  end

  for version <- 3..5 do
    test "v#{version} serializes a request with topic names" do
      version = unquote(version)
      module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Request])

      request = %{
        struct(module)
        | correlation_id: 8,
          client_id: "kayrock",
          transactional_id: "txn",
          group_id: "grp",
          producer_id: 21,
          producer_epoch: 3,
          generation_id_or_member_epoch: -1,
          member_id: "",
          group_instance_id: nil,
          topics: [
            %{
              name: "t",
              partitions: [
                %{
                  partition_index: 1,
                  committed_offset: 100,
                  committed_leader_epoch: -1,
                  committed_metadata: nil,
                  tagged_fields: []
                }
              ],
              tagged_fields: []
            }
          ]
      }

      expected =
        <<@api_key::16, version::16, 8::32, 7::16, "kayrock", 0>> <>
          <<
            # transactional_id, group_id: compact strings
            4,
            "txn",
            4,
            "grp",
            # producer_id, producer_epoch
            21::64,
            3::16,
            # generation_id_or_member_epoch
            -1::32-signed,
            # member_id: empty compact string
            1,
            # group_instance_id: null compact nullable string
            0,
            # topics: compact array of 1
            2,
            2,
            "t",
            # partitions: compact array of 1
            2,
            1::32,
            100::64,
            -1::32-signed,
            # committed_metadata: null
            0,
            # partition, topic, request tagged fields
            0,
            0,
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v#{version} deserializes a response with topic names" do
      version = unquote(version)
      module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])

      binary =
        <<
          8::32,
          0,
          # throttle_time_ms
          0::32,
          # topics: 1
          2,
          2,
          "t",
          # partitions: (1, NONE), (2, UNKNOWN_MEMBER_ID 25)
          3,
          1::32,
          0::16,
          0,
          2::32,
          25::16,
          0,
          0,
          0
        >>

      assert module.deserialize(binary) ==
               {%{
                  struct(module)
                  | correlation_id: 8,
                    throttle_time_ms: 0,
                    topics: [
                      %{
                        name: "t",
                        partitions: [
                          %{partition_index: 1, error_code: 0, tagged_fields: []},
                          %{partition_index: 2, error_code: 25, tagged_fields: []}
                        ],
                        tagged_fields: []
                      }
                    ],
                    tagged_fields: []
                }, <<>>}
    end
  end

  describe "v6 (topic ids)" do
    alias Kayrock.TxnOffsetCommit.V6

    test "the request and response topics carry topic_id instead of name" do
      {:compact_array, topic_schema} = V6.Request.schema()[:topics]
      assert topic_schema[:topic_id] == :uuid
      refute Keyword.has_key?(topic_schema, :name)

      {:compact_array, response_topic_schema} = V6.Response.schema()[:topics]
      assert response_topic_schema[:topic_id] == :uuid
      refute Keyword.has_key?(response_topic_schema, :name)
    end

    test "serializes a request" do
      request = %V6.Request{
        correlation_id: 9,
        client_id: "kayrock",
        transactional_id: "txn",
        group_id: "grp",
        producer_id: 21,
        producer_epoch: 3,
        generation_id_or_member_epoch: 4,
        member_id: "m",
        group_instance_id: "i",
        topics: [
          %{
            topic_id: @topic_id,
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 5,
                committed_leader_epoch: 2,
                committed_metadata: "md",
                tagged_fields: []
              }
            ],
            tagged_fields: []
          }
        ]
      }

      expected =
        <<@api_key::16, 6::16, 9::32, 7::16, "kayrock", 0>> <>
          <<4, "txn", 4, "grp", 21::64, 3::16, 4::32, 2, "m", 2, "i">> <>
          <<2>> <>
          @topic_id <>
          <<2, 0::32, 5::64, 2::32, 3, "md", 0, 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "deserializes a response" do
      # UNKNOWN_TOPIC_ID (100)
      binary = <<9::32, 0, 0::32, 2>> <> @topic_id <> <<2, 0::32, 100::16, 0, 0, 0>>

      assert Kayrock.TxnOffsetCommit.deserialize(6, binary) ==
               {%V6.Response{
                  correlation_id: 9,
                  throttle_time_ms: 0,
                  topics: [
                    %{
                      topic_id: @topic_id,
                      partitions: [%{partition_index: 0, error_code: 100, tagged_fields: []}],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end
end
