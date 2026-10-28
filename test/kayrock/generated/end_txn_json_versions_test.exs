defmodule Kayrock.EndTxnJsonVersionsTest do
  @moduledoc """
  Wire fixtures for the JSON-derived EndTxn versions (v2-v5), derived by hand
  from `priv/kafka_messages/4.4.0/EndTxn{Request,Response}.json`.

  The JSON specs call the commit flag `Committed`, so v2+ use `committed`
  (kpro-sourced v0/v1 keep `transaction_result`). v3 is the first flexible
  version; v5 (KIP-890 part 2) adds ProducerId/ProducerEpoch to the response.
  """
  use ExUnit.Case, async: true

  @api_key 26

  test "generated range" do
    assert Kayrock.KafkaSchemaMetadata.version_range(:end_txn) == {0, 5}
  end

  test "v2 (non-flexible) serializes and deserializes" do
    request = %Kayrock.EndTxn.V2.Request{
      correlation_id: 2,
      client_id: "kayrock",
      transactional_id: "txn",
      producer_id: 11,
      producer_epoch: 4,
      committed: true
    }

    expected =
      <<@api_key::16, 2::16, 2::32, 7::16, "kayrock">> <> <<3::16, "txn", 11::64, 4::16, 1>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected

    assert Kayrock.EndTxn.V2.Response.deserialize(<<2::32, 0::32, 0::16>>) ==
             {%Kayrock.EndTxn.V2.Response{correlation_id: 2, throttle_time_ms: 0, error_code: 0},
              <<>>}
  end

  for version <- 3..4 do
    test "v#{version} (flexible) serializes and deserializes" do
      version = unquote(version)
      request_module = Module.concat([Kayrock.EndTxn, :"V#{version}", Request])
      response_module = Module.concat([Kayrock.EndTxn, :"V#{version}", Response])

      request = %{
        struct(request_module)
        | correlation_id: 3,
          client_id: "kayrock",
          transactional_id: "txn",
          producer_id: 11,
          producer_epoch: 4,
          committed: false
      }

      expected =
        <<@api_key::16, version::16, 3::32, 7::16, "kayrock", 0>> <>
          <<4, "txn", 11::64, 4::16, 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected

      # INVALID_TXN_STATE (48)
      assert response_module.deserialize(<<3::32, 0, 1::32, 48::16, 0>>) ==
               {%{
                  struct(response_module)
                  | correlation_id: 3,
                    throttle_time_ms: 1,
                    error_code: 48,
                    tagged_fields: []
                }, <<>>}
    end
  end

  test "v5 serializes a commit and deserializes the bumped producer epoch" do
    request = %Kayrock.EndTxn.V5.Request{
      correlation_id: 6,
      client_id: "kayrock",
      transactional_id: "txn",
      producer_id: 11,
      producer_epoch: 4,
      committed: true
    }

    expected =
      <<@api_key::16, 5::16, 6::32, 7::16, "kayrock", 0>> <> <<4, "txn", 11::64, 4::16, 1, 0>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected

    binary =
      <<
        # correlation_id, header tag buffer
        6::32,
        0,
        # throttle_time_ms, error_code
        0::32,
        0::16,
        # producer_id, producer_epoch
        11::64,
        5::16,
        # tagged fields
        0
      >>

    assert Kayrock.EndTxn.deserialize(5, binary) ==
             {%Kayrock.EndTxn.V5.Response{
                correlation_id: 6,
                throttle_time_ms: 0,
                error_code: 0,
                producer_id: 11,
                producer_epoch: 5,
                tagged_fields: []
              }, <<>>}
  end
end
