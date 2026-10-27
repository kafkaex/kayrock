defmodule Kayrock.Kafka4.TelemetryTest do
  @moduledoc """
  KIP-714 client telemetry against `apache/kafka:4.4.0`:
  GetTelemetrySubscriptions (71) v0 and PushTelemetry (72) v0.

  The broker only *advertises* these APIs when a client telemetry exporter
  plugin is configured (a `metric.reporters` class implementing
  `ClientTelemetryExporterProvider`), and the `apache/kafka` image ships
  none, so ApiVersions omits keys 71 and 72. The broker still answers both
  requests: GetTelemetrySubscriptions returns a broker-assigned client
  instance id with an empty subscription (`requested_metrics: []`), and
  PushTelemetry is accepted.

  Every response is checked to decode completely (no leftover bytes).
  """
  use Kayrock.Kafka4Case

  alias Kayrock.GetTelemetrySubscriptions
  alias Kayrock.Kafka4.RawCall
  alias Kayrock.PushTelemetry

  setup %{kafka: kafka} do
    {:ok, conn} = connect_broker(kafka)
    {:ok, conn: conn}
  end

  test "the broker does not advertise the telemetry APIs without an exporter", %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    broker = broker_api_versions(client)

    refute Map.has_key?(broker, 71)
    refute Map.has_key?(broker, 72)
  end

  describe "GetTelemetrySubscriptions v0" do
    test "a first request (zero client instance id) gets an assigned id", %{conn: conn} do
      response =
        RawCall.call!(conn, %GetTelemetrySubscriptions.V0.Request{
          correlation_id: 1,
          client_id: "kayrock-kafka4",
          client_instance_id: nil
        })

      assert %GetTelemetrySubscriptions.V0.Response{error_code: 0, tagged_fields: []} = response
      assert <<_::128>> = response.client_instance_id
      refute response.client_instance_id == <<0::128>>
      assert is_integer(response.subscription_id)
      # no metrics reporter: nothing is subscribed
      assert response.requested_metrics == []
      assert response.push_interval_ms > 0
      assert response.telemetry_max_bytes > 0
      assert response.delta_temporality in [0, 1]

      # compression types the broker accepts for PushTelemetry (CompressionType ids)
      assert response.accepted_compression_types != []
      assert Enum.all?(response.accepted_compression_types, &(&1 in 1..4))
    end

    test "an immediate repeat request is throttled", %{conn: conn} do
      first =
        RawCall.call!(conn, %GetTelemetrySubscriptions.V0.Request{
          correlation_id: 1,
          client_id: "kayrock-kafka4",
          client_instance_id: nil
        })

      again =
        RawCall.call!(conn, %GetTelemetrySubscriptions.V0.Request{
          correlation_id: 2,
          client_id: "kayrock-kafka4",
          client_instance_id: first.client_instance_id
        })

      # KIP-714: a client asking again before the push interval has elapsed
      # gets THROTTLING_QUOTA_EXCEEDED (89); the response still decodes fully
      # and, since an id was sent, carries the zero id ("else 0")
      assert again.error_code == 89
      assert again.client_instance_id == <<0::128>>
    end
  end

  describe "PushTelemetry v0" do
    test "a push for the current subscription decodes", %{conn: conn} do
      subscription =
        RawCall.call!(conn, %GetTelemetrySubscriptions.V0.Request{
          correlation_id: 1,
          client_id: "kayrock-kafka4",
          client_instance_id: nil
        })

      response =
        RawCall.call!(conn, %PushTelemetry.V0.Request{
          correlation_id: 2,
          client_id: "kayrock-kafka4",
          client_instance_id: subscription.client_instance_id,
          subscription_id: subscription.subscription_id,
          terminating: false,
          compression_type: 0,
          metrics: ""
        })

      assert %PushTelemetry.V0.Response{error_code: 0, tagged_fields: []} = response
      assert response.throttle_time_ms >= 0
    end

    test "an unknown client instance id gets UNKNOWN_SUBSCRIPTION_ID", %{conn: conn} do
      response =
        RawCall.call!(conn, %PushTelemetry.V0.Request{
          correlation_id: 3,
          client_id: "kayrock-kafka4",
          client_instance_id: :crypto.strong_rand_bytes(16),
          subscription_id: 1,
          terminating: true,
          compression_type: 0,
          metrics: ""
        })

      assert %PushTelemetry.V0.Response{tagged_fields: []} = response
      # KIP-714: a push for a subscription the broker does not know gets
      # UNKNOWN_SUBSCRIPTION_ID (117), which is what 4.4.0 returns here.
      assert response.error_code == 117
    end
  end
end
