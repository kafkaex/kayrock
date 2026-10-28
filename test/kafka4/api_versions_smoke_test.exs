defmodule Kayrock.Kafka4.ApiVersionsSmokeTest do
  @moduledoc """
  Smoke test of the Kafka 4.x harness: ApiVersions against `apache/kafka:4.4.0`
  succeeds, and every API Kayrock generates overlaps the range the broker
  advertises.

  Kafka 4.0 removed old protocol versions (KIP-896). Kayrock keeps generating
  them (older brokers and downstream pattern matches need them), so the
  versions a 4.4.0 broker no longer advertises are listed explicitly in
  `@removed_by_broker`. The test fails if that list drifts in either
  direction, so a change in broker support (or in what Kayrock generates) has
  to be acknowledged here.

  Observed with apache/kafka:4.4.0 (single KRaft node, default features):

  | API (key)                    | Kayrock | Broker | Not accepted by 4.4.0 |
  |------------------------------|---------|--------|-----------------------|
  | Produce (0)                  | 0-13    | 0-13   | (0-2 advertised but not answered) |
  | Fetch (1)                    | 0-18    | 4-18   | 0-3                   |
  | ListOffsets (2)              | 0-11    | 1-11   | 0                     |
  | Metadata (3)                 | 0-13    | 0-13   |                       |
  | OffsetCommit (8)             | 0-10    | 2-10   | 0-1                   |
  | OffsetFetch (9)              | 0-10    | 1-10   | 0                     |
  | FindCoordinator (10)         | 0-6     | 0-6    |                       |
  | JoinGroup (11)               | 0-9     | 0-9    |                       |
  | Heartbeat (12)               | 0-4     | 0-4    |                       |
  | LeaveGroup (13)              | 0-5     | 0-5    |                       |
  | SyncGroup (14)               | 0-5     | 0-5    |                       |
  | DescribeGroups (15)          | 0-6     | 0-6    |                       |
  | ListGroups (16)              | 0-5     | 0-5    |                       |
  | SaslHandshake (17)           | 0-1     | 0-1    |                       |
  | ApiVersions (18)             | 0-3     | 0-5    |                       |
  | CreateTopics (19)            | 0-5     | 2-7    | 0-1                   |
  | DeleteTopics (20)            | 0-4     | 1-6    | 0                     |
  | DeleteRecords (21)           | 0-2     | 0-2    |                       |
  | InitProducerId (22)          | 0-5     | 0-5    |                       |
  | OffsetForLeaderEpoch (23)    | 0-4     | 2-4    | 0-1                   |
  | AddPartitionsToTxn (24)      | 0-3     | 0-5    |                       |
  | AddOffsetsToTxn (25)         | 0-4     | 0-4    |                       |
  | EndTxn (26)                  | 0-5     | 0-5    |                       |
  | TxnOffsetCommit (28)         | 0-6     | 0-6    |                       |
  | DescribeAcls (29)            | 0-1     | 1-3    | 0                     |
  | CreateAcls (30)              | 0-1     | 1-3    | 0                     |
  | DeleteAcls (31)              | 0-1     | 1-3    | 0                     |
  | DescribeConfigs (32)         | 0-2     | 1-4    | 0                     |
  | AlterConfigs (33)            | 0-1     | 0-2    |                       |
  | AlterReplicaLogDirs (34)     | 0-1     | 1-2    | 0                     |
  | DescribeLogDirs (35)         | 0-1     | 1-5    | 0                     |
  | SaslAuthenticate (36)        | 0-1     | 0-2    |                       |
  | CreatePartitions (37)        | 0-1     | 0-3    |                       |
  | CreateDelegationToken (38)   | 0-2     | 1-3    | 0                     |
  | RenewDelegationToken (39)    | 0-1     | 1-2    | 0                     |
  | ExpireDelegationToken (40)   | 0-1     | 1-2    | 0                     |
  | DescribeDelegationToken (41) | 0-1     | 1-3    | 0                     |
  | DeleteGroups (42)            | 0-3     | 0-3    |                       |
  | ElectLeaders (43)            | 0-2     | 0-2    |                       |
  | IncrementalAlterConfigs (44) | 0-1     | 0-1    |                       |
  | AlterPartitionReassignments (45) | 0   | 0-1    |                       |
  | ListPartitionReassignments (46)  | 0   | 0      |                       |
  | OffsetDelete (47)            | 0       | 0      |                       |
  | ConsumerGroupHeartbeat (68)  | 0-1     | 0-1    |                       |
  | ConsumerGroupDescribe (69)   | 0-1     | 0-1    |                       |
  | DescribeProducers (61)       | 0       | 0      |                       |
  | DescribeTransactions (65)    | 0       | 0      |                       |
  | ListTransactions (66)        | 0-2     | 0-2    |                       |
  | ShareGroupHeartbeat (76)     | 1       | 1      |                       |
  | ShareGroupDescribe (77)      | 1       | 1      |                       |
  | ShareFetch (78)              | 1-2     | 1-2    |                       |
  | ShareAcknowledge (79)        | 1-2     | 1-2    |                       |
  | DescribeShareGroupOffsets (90) | 0-1   | 0-1    |                       |
  | AlterShareGroupOffsets (91)  | 0       | 0      |                       |
  | DeleteShareGroupOffsets (92) | 0       | 0      |                       |
  | GetTelemetrySubscriptions (71) | 0     | -      | (0 answered, not advertised) |
  | PushTelemetry (72)           | 0       | -      | (0 answered, not advertised) |
  | StreamsGroupHeartbeat (88)   | 0-1     | 0-1    |                       |
  | StreamsGroupDescribe (89)    | 0-1     | 0-1    |                       |
  | StreamsGroupTopologyDescriptionUpdate (93) | 0 | 0 |                  |

  GetTelemetrySubscriptions and PushTelemetry (KIP-714) are only advertised
  when the broker has a client telemetry exporter plugin (`metric.reporters`
  implementing `ClientTelemetryExporterProvider`), and the `apache/kafka`
  image ships none. The broker still answers them, so they are listed in
  `@removed_by_broker` here and in `@answered_but_not_advertised` in
  `Kayrock.Kafka4.VersionCompatibilityTest`.

  Produce v0-v2 are a special case: Kafka 4.x still advertises them but
  does not answer them (KIP-896 keeps them advertised only for old-client
  negotiation); `Kayrock.Kafka4.VersionCompatibilityTest` pins that down.

  A request at a version the broker does not accept gets no response: the
  broker closes the connection, which `Kayrock.Client` surfaces as a recv
  timeout / `:closed` error.

  Run with `mix test.kafka4`.
  """
  use Kayrock.Kafka4Case

  # {api, versions Kayrock generates that apache/kafka:4.4.0 does not advertise}
  @removed_by_broker %{
    fetch: 0..3,
    list_offsets: 0..0,
    offset_commit: 0..1,
    offset_fetch: 0..0,
    create_topics: 0..1,
    delete_topics: 0..0,
    offset_for_leader_epoch: 0..1,
    describe_acls: 0..0,
    create_acls: 0..0,
    delete_acls: 0..0,
    describe_configs: 0..0,
    alter_replica_log_dirs: 0..0,
    describe_log_dirs: 0..0,
    create_delegation_token: 0..0,
    renew_delegation_token: 0..0,
    expire_delegation_token: 0..0,
    describe_delegation_token: 0..0,
    # not advertised without a client telemetry exporter plugin
    get_telemetry_subscriptions: 0..0,
    push_telemetry: 0..0
  }

  describe "ApiVersions against Kafka 4.4.0" do
    for version <- 0..3 do
      test "v#{version} succeeds", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request =
          case Kayrock.ApiVersions.get_request_struct(version) do
            %{client_software_name: _} = request ->
              %{request | client_software_name: "kayrock", client_software_version: "test"}

            request ->
              request
          end

        {:ok, response} = Kayrock.client_call(client, request, :random)

        assert response.error_code == 0
        assert length(response.api_keys) > length(Kayrock.KafkaSchemaMetadata.all_apis())

        for %{min_version: min, max_version: max} <- response.api_keys do
          assert min <= max
        end
      end
    end

    test "every generated API intersects the broker's advertised range", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      broker = broker_api_versions(client)

      for api <- Kayrock.KafkaSchemaMetadata.all_apis(),
          {kayrock_min, kayrock_max} = Kayrock.KafkaSchemaMetadata.version_range(api),
          Map.get(@removed_by_broker, api) != kayrock_min..kayrock_max//1 do
        api_key = Kayrock.KafkaSchemaMetadata.api_key(api)

        assert {broker_min, broker_max} = Map.get(broker, api_key),
               "broker does not advertise #{api} (key #{api_key})"

        assert max(kayrock_min, broker_min) <= min(kayrock_max, broker_max),
               "#{api}: Kayrock #{kayrock_min}..#{kayrock_max} does not intersect " <>
                 "broker #{broker_min}..#{broker_max}"
      end
    end

    test "versions the broker no longer advertises match the KIP-896 list", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      broker = broker_api_versions(client)

      actual =
        for api <- Kayrock.KafkaSchemaMetadata.all_apis(),
            {min, max} = Kayrock.KafkaSchemaMetadata.version_range(api),
            unsupported = Enum.reject(min..max, &broker_supports?(broker, api, &1)),
            unsupported != [],
            into: %{} do
          {api, List.first(unsupported)..List.last(unsupported)//1}
        end

      assert actual == @removed_by_broker
    end

    test "the removed list only names APIs and versions Kayrock generates" do
      for {api, versions} <- @removed_by_broker do
        assert api in Kayrock.KafkaSchemaMetadata.all_apis()
        {min, max} = Kayrock.KafkaSchemaMetadata.version_range(api)
        assert versions.first >= min and versions.last <= max
      end
    end
  end
end
