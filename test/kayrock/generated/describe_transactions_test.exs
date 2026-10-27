defmodule Kayrock.DescribeTransactionsTest do
  @moduledoc """
  Wire fixtures for DescribeTransactions (API key 65, v0, KIP-664), derived
  by hand from
  `priv/kafka_messages/4.4.0/DescribeTransactions{Request,Response}.json`.

  v0 is flexible: request header v2, response header v1.
  """
  use ExUnit.Case, async: true

  alias Kayrock.DescribeTransactions.V0

  test "module metadata" do
    assert Kayrock.DescribeTransactions.min_vsn() == 0
    assert Kayrock.DescribeTransactions.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_transactions) == 65
    assert Kayrock.KafkaSchemaMetadata.api_key(65) == :describe_transactions
    assert Kayrock.DescribeTransactions.get_request_struct(0) == %V0.Request{}
  end

  test "serializes a request" do
    request = %V0.Request{correlation_id: 4, client_id: "kayrock", transactional_ids: ["a", "bc"]}

    expected =
      <<65::16, 0::16, 4::32, 7::16, "kayrock", 0>> <>
        <<
          # transactional_ids: compact array of 2 compact strings
          3,
          2,
          "a",
          3,
          "bc",
          0
        >>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "serializes an empty request" do
    request = %V0.Request{correlation_id: 0, client_id: nil, transactional_ids: []}

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<65::16, 0::16, 0::32, -1::16-signed, 0, 1, 0>>
  end

  test "deserializes a response" do
    binary =
      <<
        # correlation_id, header tag buffer
        4::32,
        0,
        # throttle_time_ms
        0::32,
        # transaction_states: 2
        3,
        # 1: ongoing transaction over t1 [0, 1]
        0::16,
        2,
        "a",
        8,
        "Ongoing",
        60_000::32,
        1_700_000_000_000::64,
        5::64,
        1::16,
        # topics: 1
        2,
        3,
        "t1",
        3,
        0::32,
        1::32,
        0,
        0,
        # 2: TRANSACTIONAL_ID_NOT_FOUND (105)
        105::16,
        3,
        "bc",
        1,
        0::32,
        -1::64-signed,
        -1::64-signed,
        -1::16-signed,
        1,
        0,
        # response tagged fields
        0
      >>

    assert Kayrock.DescribeTransactions.deserialize(0, binary) ==
             {%V0.Response{
                correlation_id: 4,
                throttle_time_ms: 0,
                transaction_states: [
                  %{
                    error_code: 0,
                    transactional_id: "a",
                    transaction_state: "Ongoing",
                    transaction_timeout_ms: 60_000,
                    transaction_start_time_ms: 1_700_000_000_000,
                    producer_id: 5,
                    producer_epoch: 1,
                    topics: [%{topic: "t1", partitions: [0, 1], tagged_fields: []}],
                    tagged_fields: []
                  },
                  %{
                    error_code: 105,
                    transactional_id: "bc",
                    transaction_state: "",
                    transaction_timeout_ms: 0,
                    transaction_start_time_ms: -1,
                    producer_id: -1,
                    producer_epoch: -1,
                    topics: [],
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end
end
