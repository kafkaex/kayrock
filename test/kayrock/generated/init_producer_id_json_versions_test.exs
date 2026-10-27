defmodule Kayrock.InitProducerIdJsonVersionsTest do
  @moduledoc """
  Wire fixtures for the JSON-derived InitProducerId versions (v3-v5), derived
  by hand from `priv/kafka_messages/4.4.0/InitProducerId{Request,Response}.json`.

  v3 adds ProducerId/ProducerEpoch to the request (KIP-360); v4 and v5 only
  add error codes (PRODUCER_FENCED, TRANSACTION_ABORTABLE), so their layout
  is the same as v3. v6 (KIP-939, 2PC) is `latestVersionUnstable` in 4.4.0
  and is not generated.

  All versions are flexible: request header v2 (client_id is a plain int16
  nullable string, then an empty tag buffer), response header v1.
  """
  use ExUnit.Case, async: true

  @api_key 22

  test "the generated range stops below the unstable v6" do
    assert Kayrock.InitProducerId.min_vsn() == 0
    assert Kayrock.InitProducerId.max_vsn() == 5
    assert Kayrock.KafkaSchemaMetadata.version_range(:init_producer_id) == {0, 5}
  end

  for version <- 3..5 do
    describe "v#{version}" do
      @version version

      test "serializes a transactional request" do
        module = Module.concat([Kayrock.InitProducerId, :"V#{@version}", Request])

        request = %{
          struct(module)
          | correlation_id: 7,
            client_id: "kayrock",
            transactional_id: "txn-1",
            transaction_timeout_ms: 60_000,
            producer_id: 1234,
            producer_epoch: 5
        }

        expected =
          <<@api_key::16, @version::16, 7::32, 7::16, "kayrock", 0>> <>
            <<
              # transactional_id: compact nullable string (length + 1)
              6,
              "txn-1",
              # transaction_timeout_ms
              60_000::32,
              # producer_id
              1234::64,
              # producer_epoch
              5::16,
              # tagged fields
              0
            >>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
        assert Kayrock.Request.api_vsn(request) == @version
      end

      test "serializes a non-transactional (null id) request with the -1 defaults" do
        module = Module.concat([Kayrock.InitProducerId, :"V#{@version}", Request])

        request = %{
          struct(module)
          | correlation_id: 1,
            client_id: nil,
            transactional_id: nil,
            transaction_timeout_ms: 0,
            producer_id: -1,
            producer_epoch: -1
        }

        expected =
          <<@api_key::16, @version::16, 1::32, -1::16-signed, 0>> <>
            <<0, 0::32, -1::64-signed, -1::16-signed, 0>>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      end

      test "deserializes a response" do
        module = Module.concat([Kayrock.InitProducerId, :"V#{@version}", Response])

        binary =
          <<
            # correlation_id, empty header tag buffer
            9::32,
            0,
            # throttle_time_ms
            25::32,
            # error_code
            0::16,
            # producer_id
            4242::64,
            # producer_epoch
            3::16,
            # tagged fields
            0
          >>

        assert {response, <<>>} = module.deserialize(binary)

        assert response == %{
                 struct(module)
                 | correlation_id: 9,
                   throttle_time_ms: 25,
                   error_code: 0,
                   producer_id: 4242,
                   producer_epoch: 3,
                   tagged_fields: []
               }

        assert Kayrock.InitProducerId.deserialize(@version, binary) == {response, <<>>}
      end

      test "deserializes an error response with unknown tagged fields" do
        module = Module.concat([Kayrock.InitProducerId, :"V#{@version}", Response])

        # PRODUCER_FENCED (90); one unknown tag 7 with 2 bytes
        binary = <<1::32, 0, 0::32, 90::16, -1::64-signed, -1::16-signed, 1, 7, 2, 0xAB, 0xCD>>

        assert {response, <<>>} = module.deserialize(binary)
        assert response.error_code == 90
        assert response.producer_id == -1
        assert response.producer_epoch == -1
        assert response.tagged_fields == [{7, <<0xAB, 0xCD>>}]
      end
    end
  end

  test "get_request_struct covers every generated version" do
    for version <- 0..5 do
      assert %{__struct__: module} = Kayrock.InitProducerId.get_request_struct(version)
      assert module.api_vsn() == version
    end
  end
end
