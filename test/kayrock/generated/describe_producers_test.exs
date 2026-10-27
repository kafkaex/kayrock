defmodule Kayrock.DescribeProducersTest do
  @moduledoc """
  Wire fixtures for DescribeProducers (API key 61, v0, KIP-664), derived by
  hand from `priv/kafka_messages/4.4.0/DescribeProducers{Request,Response}.json`.

  v0 is flexible: request header v2, response header v1.
  """
  use ExUnit.Case, async: true

  alias Kayrock.DescribeProducers.V0

  test "module metadata" do
    assert Kayrock.DescribeProducers.min_vsn() == 0
    assert Kayrock.DescribeProducers.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_producers) == 61
    assert Kayrock.KafkaSchemaMetadata.api_key(61) == :describe_producers
    assert Kayrock.DescribeProducers.get_request_struct(0) == %V0.Request{}
  end

  test "serializes a request" do
    request = %V0.Request{
      correlation_id: 12,
      client_id: "kayrock",
      topics: [
        %{name: "t1", partition_indexes: [0, 1], tagged_fields: []},
        %{name: "t2", partition_indexes: [], tagged_fields: []}
      ]
    }

    expected =
      <<61::16, 0::16, 12::32, 7::16, "kayrock", 0>> <>
        <<
          # topics: compact array of 2
          3,
          3,
          "t1",
          # partition_indexes: compact array of 2 int32
          3,
          0::32,
          1::32,
          0,
          3,
          "t2",
          1,
          0,
          # request tagged fields
          0
        >>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    assert Kayrock.Request.response_deserializer(request) == (&V0.Response.deserialize/1)
  end

  test "deserializes a response" do
    binary =
      <<
        # correlation_id, header tag buffer
        12::32,
        0,
        # throttle_time_ms
        0::32,
        # topics: 1
        2,
        3,
        "t1",
        # partitions: 2
        3,
        # partition 0: no error, null message, 1 active producer
        0::32,
        0::16,
        0,
        2,
        # producer_id, producer_epoch (int32), last_sequence, last_timestamp
        1000::64,
        2::32,
        7::32,
        1_700_000_000_000::64,
        # coordinator_epoch, current_txn_start_offset
        -1::32-signed,
        42::64,
        0,
        0,
        # partition 1: NOT_LEADER_OR_FOLLOWER (6) with a message, no producers
        1::32,
        6::16,
        5,
        "oops",
        1,
        0,
        # topic tagged fields
        0,
        # response tagged fields
        0
      >>

    assert Kayrock.DescribeProducers.deserialize(0, binary) ==
             {%V0.Response{
                correlation_id: 12,
                throttle_time_ms: 0,
                topics: [
                  %{
                    name: "t1",
                    partitions: [
                      %{
                        partition_index: 0,
                        error_code: 0,
                        error_message: nil,
                        active_producers: [
                          %{
                            producer_id: 1000,
                            producer_epoch: 2,
                            last_sequence: 7,
                            last_timestamp: 1_700_000_000_000,
                            coordinator_epoch: -1,
                            current_txn_start_offset: 42,
                            tagged_fields: []
                          }
                        ],
                        tagged_fields: []
                      },
                      %{
                        partition_index: 1,
                        error_code: 6,
                        error_message: "oops",
                        active_producers: [],
                        tagged_fields: []
                      }
                    ],
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end
end
