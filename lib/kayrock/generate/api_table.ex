defmodule Kayrock.Generate.ApiTable do
  @moduledoc """
  The single, reviewable list of Kafka APIs Kayrock generates and the highest
  version generated for each.

  This table is the allowlist: an API that is not listed here is not
  generated, and versions above the listed maximum are not generated. Raising
  a maximum (or adding an API) here and running `mix gen.kafka_protocol` is how
  new protocol support is added.

  ## Where schemas come from

  Each version of each API is sourced as follows (see
  `Kayrock.Generate.SchemaSource`):

    * versions up to and including `kpro_max_versions/0` (what
      `kafka_protocol`'s `:kpro_schema` defines, i.e. everything Kayrock
      generated before the JSON-driven generator existed) are read from
      `:kpro_schema`, so their generated code stays byte-for-byte identical;
    * versions above that are translated from the vendored Apache Kafka JSON
      message specs (`priv/kafka_messages/<kafka_spec_version/0>/`) by
      `Kayrock.Generate.JsonSpec`.

  Kafka 4.x removed many old versions from its JSON specs (KIP-896), which is
  another reason old versions keep coming from `:kpro_schema`.

  Only versions that are stable in the JSON specs (not covered by
  `"latestVersionUnstable": true`) may be listed; `Kayrock.Generate.SchemaSource`
  validates this at generation time.
  """

  @kafka_spec_version "4.4.0"

  # The order of this list is the order of `Kayrock.KafkaSchemaMetadata.all_apis/0`
  # (Kafka API key order, matching `:kpro_schema.all_apis/0`).
  #
  # {api, max version generated}
  @max_versions [
    produce: 13,
    fetch: 18,
    list_offsets: 11,
    metadata: 13,
    offset_commit: 10,
    offset_fetch: 10,
    find_coordinator: 6,
    join_group: 9,
    heartbeat: 4,
    leave_group: 5,
    sync_group: 5,
    describe_groups: 6,
    list_groups: 5,
    sasl_handshake: 1,
    api_versions: 5,
    create_topics: 7,
    delete_topics: 6,
    delete_records: 2,
    init_producer_id: 5,
    offset_for_leader_epoch: 4,
    add_partitions_to_txn: 3,
    add_offsets_to_txn: 4,
    end_txn: 5,
    txn_offset_commit: 6,
    describe_acls: 3,
    create_acls: 3,
    delete_acls: 3,
    describe_configs: 4,
    alter_configs: 2,
    alter_replica_log_dirs: 2,
    describe_log_dirs: 5,
    sasl_authenticate: 2,
    create_partitions: 3,
    create_delegation_token: 3,
    renew_delegation_token: 2,
    expire_delegation_token: 2,
    describe_delegation_token: 3,
    delete_groups: 3,
    elect_leaders: 2,
    incremental_alter_configs: 1,
    alter_partition_reassignments: 1,
    list_partition_reassignments: 0,
    offset_delete: 0,
    consumer_group_heartbeat: 1,
    consumer_group_describe: 1,
    describe_quorum: 2,
    update_features: 2,
    describe_cluster: 2,
    unregister_broker: 0,
    list_config_resources: 1,
    describe_topic_partitions: 0,
    add_raft_voter: 1,
    remove_raft_voter: 0,
    describe_client_quotas: 1,
    alter_client_quotas: 1,
    describe_user_scram_credentials: 0,
    alter_user_scram_credentials: 0,
    describe_producers: 0,
    describe_transactions: 0,
    list_transactions: 2,
    share_group_heartbeat: 1,
    share_group_describe: 1,
    share_fetch: 2,
    share_acknowledge: 2,
    describe_share_group_offsets: 1,
    alter_share_group_offsets: 0,
    delete_share_group_offsets: 0
  ]

  # The version ranges defined by kafka_protocol 4.3.x's :kpro_schema.  These
  # are frozen: they define which versions are sourced from :kpro_schema and
  # must not change when the kafka_protocol dependency changes.
  @kpro_max_versions [
    produce: 8,
    fetch: 11,
    list_offsets: 5,
    metadata: 9,
    offset_commit: 8,
    offset_fetch: 6,
    find_coordinator: 3,
    join_group: 6,
    heartbeat: 4,
    leave_group: 4,
    sync_group: 4,
    describe_groups: 5,
    list_groups: 3,
    sasl_handshake: 1,
    api_versions: 3,
    create_topics: 5,
    delete_topics: 4,
    delete_records: 1,
    init_producer_id: 2,
    offset_for_leader_epoch: 3,
    add_partitions_to_txn: 1,
    add_offsets_to_txn: 1,
    end_txn: 1,
    txn_offset_commit: 2,
    describe_acls: 1,
    create_acls: 1,
    delete_acls: 1,
    describe_configs: 2,
    alter_configs: 1,
    alter_replica_log_dirs: 1,
    describe_log_dirs: 1,
    sasl_authenticate: 1,
    create_partitions: 1,
    create_delegation_token: 2,
    renew_delegation_token: 1,
    expire_delegation_token: 1,
    describe_delegation_token: 1,
    delete_groups: 2,
    elect_leaders: 2,
    incremental_alter_configs: 1,
    alter_partition_reassignments: 0,
    list_partition_reassignments: 0,
    offset_delete: 0
  ]

  @doc "The Apache Kafka release whose JSON message specs are vendored"
  @spec kafka_spec_version :: String.t()
  def kafka_spec_version, do: @kafka_spec_version

  @doc "Directory holding the vendored JSON message specs"
  @spec spec_dir :: Path.t()
  def spec_dir do
    Path.join([to_string(:code.priv_dir(:kayrock)), "kafka_messages", @kafka_spec_version])
  end

  @doc "Allowlisted APIs and the highest version generated for each, in generation order"
  @spec max_versions :: [{atom(), non_neg_integer()}]
  def max_versions, do: @max_versions

  @doc "Allowlisted APIs, in generation order"
  @spec apis :: [atom()]
  def apis, do: Keyword.keys(@max_versions)

  @doc "The highest version generated for `api`"
  @spec max_version(atom()) :: non_neg_integer()
  def max_version(api), do: Keyword.fetch!(@max_versions, api)

  @doc """
  The frozen per-API maximum versions sourced from `:kpro_schema`.

  Versions at or below these maxima are read from `:kpro_schema`; higher
  versions come from the JSON specs.
  """
  @spec kpro_max_versions :: [{atom(), non_neg_integer()}]
  def kpro_max_versions, do: @kpro_max_versions

  @doc "The highest `:kpro_schema`-sourced version of `api`, or `nil` for JSON-only APIs"
  @spec kpro_max_version(atom()) :: non_neg_integer() | nil
  def kpro_max_version(api), do: Keyword.get(@kpro_max_versions, api)
end
