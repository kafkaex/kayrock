defmodule Kayrock.PushTelemetryTest do
  @moduledoc """
  Tests for PushTelemetry (API key 72, KIP-714), v0.

  Generated from the Apache Kafka 4.4.0 JSON spec. v0 is flexible: request
  header v2, response header v1, compact encodings and tag buffers.

  Wire fixtures are derived by hand from
  `priv/kafka_messages/4.4.0/PushTelemetry{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.PushTelemetry
  alias Kayrock.PushTelemetry.V0.Request
  alias Kayrock.PushTelemetry.V0.Response

  @instance_id <<1::64, 2::64>>

  test "version metadata" do
    assert PushTelemetry.min_vsn() == 0
    assert PushTelemetry.max_vsn() == 0
    assert Kayrock.KafkaSchemaMetadata.api_key(:push_telemetry) == 72
    assert Kayrock.KafkaSchemaMetadata.version_range(:push_telemetry) == {0, 0}
    assert %Request{} = PushTelemetry.get_request_struct(0)
    assert Request.api_key() == 72
    assert Request.api_vsn() == 0
  end

  describe "v0 request" do
    test "serializes every field" do
      # metrics is opaque (OpenTelemetry MetricsData protobuf); any bytes do
      metrics = <<10, 3, 1, 2, 3>>

      request = %Request{
        correlation_id: 5,
        client_id: "kayrock",
        client_instance_id: @instance_id,
        subscription_id: -1_234,
        terminating: true,
        compression_type: 1,
        metrics: metrics
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               IO.iodata_to_binary([
                 # request header v2
                 <<72::16, 0::16, 5::32, 7::16, "kayrock", 0>>,
                 # client_instance_id
                 @instance_id,
                 # subscription_id
                 <<-1_234::32-signed>>,
                 # terminating: true
                 <<1>>,
                 # compression_type: gzip
                 <<1>>,
                 # metrics: compact bytes (length + 1)
                 <<6>>,
                 metrics,
                 # tag buffer
                 <<0>>
               ])
    end

    test "empty metrics payload" do
      request = %Request{
        correlation_id: 5,
        client_id: "k",
        client_instance_id: @instance_id,
        subscription_id: 0,
        terminating: false,
        compression_type: 0,
        metrics: ""
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               <<72::16, 0::16, 5::32, 1::16, "k", 0>> <>
                 @instance_id <> <<0::32, 0, 0, 1, 0>>
    end

    test "the response deserializer is the v0 response" do
      assert Kayrock.Request.response_deserializer(%Request{}) == (&Response.deserialize/1)
    end
  end

  describe "v0 response" do
    test "decodes success with no leftover bytes" do
      # correlation_id + header tag buffer, throttle_time_ms, error_code, tag buffer
      wire = <<3::32, 0, 0::32, 0::16, 0>>

      assert {response, <<>>} = Response.deserialize(wire)

      assert response == %Response{
               correlation_id: 3,
               throttle_time_ms: 0,
               error_code: 0,
               tagged_fields: []
             }
    end

    test "decodes an error code and throttle time" do
      # TELEMETRY_TOO_LARGE (118)
      wire = <<3::32, 0, 250::32, 118::16, 0>>

      assert {%Response{throttle_time_ms: 250, error_code: 118}, <<>>} =
               PushTelemetry.deserialize(0, wire)
    end
  end
end
