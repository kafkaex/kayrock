defmodule Kayrock.GetTelemetrySubscriptionsTest do
  @moduledoc """
  Tests for GetTelemetrySubscriptions (API key 71, KIP-714), v0.

  Generated from the Apache Kafka 4.4.0 JSON spec. v0 is flexible: request
  header v2, response header v1, compact encodings and tag buffers.

  Wire fixtures are derived by hand from
  `priv/kafka_messages/4.4.0/GetTelemetrySubscriptions{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.GetTelemetrySubscriptions
  alias Kayrock.GetTelemetrySubscriptions.V0.Request
  alias Kayrock.GetTelemetrySubscriptions.V0.Response

  @instance_id <<0x99, 0xF3, 0x66, 0x1D, 0x47, 0x81, 0x46, 0x6C, 0xB6, 0x0F, 0xFA, 0xBD, 0x64,
                 0x21, 0x5C, 0x7C>>

  test "version metadata" do
    assert GetTelemetrySubscriptions.min_vsn() == 0
    assert GetTelemetrySubscriptions.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:get_telemetry_subscriptions) == 71
    assert Kayrock.KafkaSchemaMetadata.version_range(:get_telemetry_subscriptions) == {0, 0}
    assert %Request{} = GetTelemetrySubscriptions.get_request_struct(0)
    assert Request.api_key() == 71
    assert Request.api_vsn() == 0
  end

  describe "v0 request" do
    test "first request: nil client instance id serializes as the zero UUID" do
      request = %Request{correlation_id: 3, client_id: "kayrock", client_instance_id: nil}

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               IO.iodata_to_binary([
                 # request header v2: api_key, api_version, correlation_id,
                 # client_id (int16-length string), header tag buffer
                 <<71::16, 0::16, 3::32, 7::16, "kayrock", 0>>,
                 # client_instance_id: zero uuid
                 <<0::128>>,
                 # tag buffer
                 <<0>>
               ])
    end

    test "a known client instance id is sent verbatim" do
      request = %Request{correlation_id: 3, client_id: "k", client_instance_id: @instance_id}

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               <<71::16, 0::16, 3::32, 1::16, "k", 0>> <> @instance_id <> <<0>>
    end

    test "the response deserializer is the v0 response" do
      assert Kayrock.Request.response_deserializer(%Request{}) == (&Response.deserialize/1)
    end
  end

  describe "v0 response" do
    # The response a apache/kafka:4.4.0 broker (no metrics reporter
    # configured) sent for a first GetTelemetrySubscriptions request.
    @wire IO.iodata_to_binary([
            # correlation_id + response header v1 tag buffer
            <<1::32, 0>>,
            # throttle_time_ms, error_code
            <<0::32, 0::16>>,
            # client_instance_id (assigned by the broker)
            @instance_id,
            # subscription_id
            <<-660_873_874::32-signed>>,
            # accepted_compression_types: compact array of int8 [zstd, lz4, gzip, snappy]
            <<5, 4, 3, 1, 2>>,
            # push_interval_ms, telemetry_max_bytes
            <<300_000::32, 1_048_576::32>>,
            # delta_temporality: true
            <<1>>,
            # requested_metrics: empty compact array (no metrics subscribed)
            <<1>>,
            # tag buffer
            <<0>>
          ])

    test "decodes every field with no leftover bytes" do
      assert {response, <<>>} = Response.deserialize(@wire)

      assert response == %Response{
               correlation_id: 1,
               throttle_time_ms: 0,
               error_code: 0,
               client_instance_id: @instance_id,
               subscription_id: -660_873_874,
               accepted_compression_types: [4, 3, 1, 2],
               push_interval_ms: 300_000,
               telemetry_max_bytes: 1_048_576,
               delta_temporality: 1,
               requested_metrics: [],
               tagged_fields: []
             }
    end

    test "decodes requested metric prefixes and an error code" do
      wire =
        IO.iodata_to_binary([
          <<9::32, 0>>,
          # throttle_time_ms, error_code UNKNOWN_SUBSCRIPTION_ID (117)
          <<10::32, 117::16>>,
          <<0::128>>,
          <<7::32>>,
          # accepted_compression_types: []
          <<1>>,
          <<1000::32, 4096::32>>,
          # delta_temporality: false
          <<0>>,
          # requested_metrics: ["", "org.apache.kafka."]
          <<3, 1, 18, "org.apache.kafka.">>,
          <<0>>
        ])

      assert {response, <<>>} = GetTelemetrySubscriptions.deserialize(0, wire)
      assert response.error_code == 117
      assert response.throttle_time_ms == 10
      assert response.client_instance_id == <<0::128>>
      assert response.accepted_compression_types == []
      assert response.delta_temporality == 0
      assert response.requested_metrics == ["", "org.apache.kafka."]
    end
  end
end
