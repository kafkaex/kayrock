defmodule Kayrock.AddOffsetsToTxnJsonVersionsTest do
  @moduledoc """
  Wire fixtures for the JSON-derived AddOffsetsToTxn versions (v2-v4),
  derived by hand from
  `priv/kafka_messages/4.4.0/AddOffsetsToTxn{Request,Response}.json`.

  The layout never changes (v2/v4 only add error codes); v3 is the first
  flexible version.
  """
  use ExUnit.Case, async: true

  @api_key 25

  test "generated range" do
    assert Kayrock.KafkaSchemaMetadata.version_range(:add_offsets_to_txn) == {0, 4}
  end

  test "v2 (non-flexible) serializes and deserializes" do
    request = %Kayrock.AddOffsetsToTxn.V2.Request{
      correlation_id: 2,
      client_id: "kayrock",
      transactional_id: "txn",
      producer_id: 9,
      producer_epoch: 1,
      group_id: "grp"
    }

    expected =
      <<@api_key::16, 2::16, 2::32, 7::16, "kayrock">> <>
        <<3::16, "txn", 9::64, 1::16, 3::16, "grp">>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected

    # PRODUCER_FENCED (90)
    assert Kayrock.AddOffsetsToTxn.V2.Response.deserialize(<<2::32, 10::32, 90::16>>) ==
             {%Kayrock.AddOffsetsToTxn.V2.Response{
                correlation_id: 2,
                throttle_time_ms: 10,
                error_code: 90
              }, <<>>}
  end

  for version <- 3..4 do
    test "v#{version} (flexible) serializes and deserializes" do
      version = unquote(version)
      request_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Request])
      response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])

      request = %{
        struct(request_module)
        | correlation_id: 5,
          client_id: "kayrock",
          transactional_id: "txn",
          producer_id: 9,
          producer_epoch: 1,
          group_id: "grp"
      }

      expected =
        <<@api_key::16, version::16, 5::32, 7::16, "kayrock", 0>> <>
          <<4, "txn", 9::64, 1::16, 4, "grp", 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected

      assert response_module.deserialize(<<5::32, 0, 0::32, 0::16, 0>>) ==
               {%{
                  struct(response_module)
                  | correlation_id: 5,
                    throttle_time_ms: 0,
                    error_code: 0,
                    tagged_fields: []
                }, <<>>}
    end
  end
end
