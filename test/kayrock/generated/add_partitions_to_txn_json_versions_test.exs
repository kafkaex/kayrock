defmodule Kayrock.AddPartitionsToTxnJsonVersionsTest do
  @moduledoc """
  Wire fixtures for the JSON-derived AddPartitionsToTxn versions (v2-v3),
  derived by hand from
  `priv/kafka_messages/4.4.0/AddPartitionsToTxn{Request,Response}.json`.

  The JSON specs name the v0-v3 fields `V3AndBelow*` (v4 replaced them with
  a batched `Transactions` array), so these versions use
  `v3_and_below_transactional_id`, `v3_and_below_topics`,
  `results_by_topic_v3_and_below`, ... (kpro-sourced v0/v1 keep their old
  names).

  v2 is non-flexible; v3 is the first flexible version. v4+ are not
  generated: the spec reserves them for brokers ("Versions 3 and below will
  be exclusively used by clients and versions 4 and above will be used by
  brokers"; they require CLUSTER_ACTION).
  """
  use ExUnit.Case, async: true

  alias Kayrock.AddPartitionsToTxn.V2
  alias Kayrock.AddPartitionsToTxn.V3

  @api_key 24

  test "client versions only (v4+ are broker-to-broker)" do
    assert Kayrock.AddPartitionsToTxn.max_vsn() == 3
    assert Kayrock.KafkaSchemaMetadata.version_range(:add_partitions_to_txn) == {0, 3}
  end

  describe "v2 (non-flexible)" do
    test "serializes a request" do
      request = %V2.Request{
        correlation_id: 3,
        client_id: "kayrock",
        v3_and_below_transactional_id: "txn",
        v3_and_below_producer_id: 77,
        v3_and_below_producer_epoch: 2,
        v3_and_below_topics: [%{name: "t1", partitions: [0, 2]}, %{name: "t2", partitions: []}]
      }

      expected =
        <<@api_key::16, 2::16, 3::32, 7::16, "kayrock">> <>
          <<
            # transactional_id: string
            3::16,
            "txn",
            # producer_id, producer_epoch
            77::64,
            2::16,
            # topics: array of 2
            2::32,
            2::16,
            "t1",
            2::32,
            0::32,
            2::32,
            2::16,
            "t2",
            0::32
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "deserializes a response" do
      binary =
        <<
          # correlation_id
          3::32,
          # throttle_time_ms
          0::32,
          # results_by_topic_v3_and_below: 1 topic
          1::32,
          2::16,
          "t1",
          # 2 partitions: (0, NONE), (2, INVALID_PRODUCER_EPOCH 47)
          2::32,
          0::32,
          0::16,
          2::32,
          47::16
        >>

      assert V2.Response.deserialize(binary) ==
               {%V2.Response{
                  correlation_id: 3,
                  throttle_time_ms: 0,
                  results_by_topic_v3_and_below: [
                    %{
                      name: "t1",
                      results_by_partition: [
                        %{partition_index: 0, partition_error_code: 0},
                        %{partition_index: 2, partition_error_code: 47}
                      ]
                    }
                  ]
                }, <<>>}
    end
  end

  describe "v3 (flexible)" do
    test "serializes a request" do
      request = %V3.Request{
        correlation_id: 4,
        client_id: "kayrock",
        v3_and_below_transactional_id: "txn",
        v3_and_below_producer_id: 77,
        v3_and_below_producer_epoch: 2,
        v3_and_below_topics: [%{name: "t1", partitions: [0, 2], tagged_fields: []}]
      }

      expected =
        <<@api_key::16, 3::16, 4::32, 7::16, "kayrock", 0>> <>
          <<
            # transactional_id: compact string
            4,
            "txn",
            77::64,
            2::16,
            # topics: compact array of 1
            2,
            3,
            "t1",
            # partitions: compact array of 2 int32
            3,
            0::32,
            2::32,
            # topic tagged fields
            0,
            # request tagged fields
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "deserializes a response" do
      binary =
        <<
          # correlation_id, header tag buffer
          4::32,
          0,
          # throttle_time_ms
          5::32,
          # results_by_topic_v3_and_below: compact array of 1
          2,
          3,
          "t1",
          # results_by_partition: compact array of 1
          2,
          1::32,
          0::16,
          0,
          # topic tagged fields
          0,
          # response tagged fields
          0
        >>

      assert V3.Response.deserialize(binary) ==
               {%V3.Response{
                  correlation_id: 4,
                  throttle_time_ms: 5,
                  results_by_topic_v3_and_below: [
                    %{
                      name: "t1",
                      results_by_partition: [
                        %{partition_index: 1, partition_error_code: 0, tagged_fields: []}
                      ],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end
end
