defmodule Kayrock.Kafka4.VersionCompatibilityTest do
  @moduledoc """
  Sends a minimal (zero-valued) request for every API version Kayrock
  generates to an `apache/kafka:4.4.0` broker, each on a fresh connection, and
  checks the broker's reaction against the KIP-896 list in
  `Kayrock.Kafka4.ApiVersionsSmokeTest`:

    * versions the broker advertises must get a response that Kayrock
      deserializes (the response may carry a Kafka error code, since the
      request content is empty or meaningless; only the framing and schema
      are being checked);
    * versions the broker no longer advertises must be rejected (no
      response: the broker closes the connection / the read times out).

  Produce v0-v2 are advertised but not answered by 4.4.0 (KIP-896 keeps
  them in ApiVersions only for old-client negotiation), so they are listed in
  `@advertised_but_rejected`.

  SaslAuthenticate is skipped: without a preceding SaslHandshake the broker
  closes the connection regardless of version.
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Helpers

  @skip [:sasl_authenticate]

  # Advertised by apache/kafka:4.4.0 but rejected on use.
  @advertised_but_rejected %{produce: [0, 1, 2]}

  # Field overrides needed for the broker to send any response at all.
  @overrides %{
    # acks=0 has no response
    produce: [acks: 1],
    fetch: [replica_id: -1],
    list_offsets: [replica_id: -1],
    offset_for_leader_epoch: [replica_id: -1],
    # UNKNOWN (0) enum values make the broker fail the request without a
    # response; use ANY (1)
    describe_acls: [
      resource_type: 1,
      operation: 1,
      permission_type: 1,
      resource_pattern_type_filter: 1
    ]
  }

  setup_all %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    broker = broker_api_versions(client)
    :ok = Kayrock.Client.stop(client)
    {:ok, broker: broker}
  end

  for api <- Kayrock.KafkaSchemaMetadata.all_apis(), api not in @skip do
    {min, max} = Kayrock.KafkaSchemaMetadata.version_range(api)

    for version <- min..max do
      test "#{api} v#{version}", %{kafka: kafka, broker: broker} do
        api = unquote(api)
        version = unquote(version)

        expected =
          if Kafka4Helpers.broker_supports?(broker, api, version) and
               version not in Map.get(@advertised_but_rejected, api, []) do
            :accepted
          else
            :rejected
          end

        request = minimal_request(api, version)

        case {expected, send_request(kafka, request)} do
          {:accepted, {:ok, response}} ->
            assert is_map(response)
            assert response.correlation_id == request.correlation_id

          {:rejected, {:error, _}} ->
            :ok

          {expected, actual} ->
            flunk("#{api} v#{version}: expected #{expected}, got #{inspect(actual, limit: 5)}")
        end
      end
    end
  end

  # Raw connection, so a rejected version (broker closes the socket) only
  # affects this request. Returns {:ok, response} or {:error, reason}.
  defp send_request(kafka, request) do
    {:ok, conn} = connect_broker(kafka)
    Process.unlink(conn)

    try do
      with :ok <- Kayrock.BrokerConnection.send(conn, Kayrock.Request.serialize(request)),
           {:ok, data} <- Kayrock.BrokerConnection.recv(conn, 5_000) do
        {response, _rest} = Kayrock.Request.response_deserializer(request).(data)
        {:ok, response}
      end
    catch
      :exit, reason -> {:error, {:exit, reason}}
    after
      Process.exit(conn, :kill)
    end
  end

  defp minimal_request(api, version) do
    module = Module.concat(Kayrock, Macro.camelize(Atom.to_string(api)))
    request = module.get_request_struct(version)
    request_module = request.__struct__

    filled =
      Enum.reduce(request_module.schema(), request, fn {field, type}, acc ->
        if Map.get(acc, field) == nil, do: Map.put(acc, field, zero(type)), else: acc
      end)

    filled
    |> struct(Map.get(@overrides, api, []))
    |> Map.merge(%{correlation_id: 7, client_id: "kayrock-kafka4"})
  end

  defp zero(type) when type in [:int8, :int16, :int32, :int64, :uint16, :varint, :varlong],
    do: 0

  defp zero(:boolean), do: false
  defp zero(type) when type in [:string, :compact_string, :bytes, :compact_bytes], do: ""
  defp zero(:tagged_fields), do: []
  defp zero({array, _}) when array in [:array, :compact_array], do: []
  defp zero(_nullable_or_unknown), do: nil
end
