defmodule Kayrock.Generate.JsonSpecTest do
  @moduledoc """
  Tests for the Apache Kafka JSON message spec -> Kayrock schema translator.

  The core cross-check compares the JSON-derived schema with `:kpro_schema`
  for every API/version both define.  `:kpro_schema` was itself generated from
  (older) Kafka JSON specs, so the two must agree on the wire layout.  The only
  differences are field *names* that Kafka renamed since, two places where
  `:kpro_schema` adds an extra nesting level that does not affect the wire
  encoding, and one known `:kpro_schema` bug (see below).
  """
  use ExUnit.Case, async: true

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.JsonSpec

  @spec_dir ApiTable.spec_dir()

  setup_all do
    {:ok, spec: JsonSpec.load!(@spec_dir)}
  end

  # APIs/versions whose JSON-derived schema is identical (names and types) to
  # :kpro_schema.  Pinned so regressions in the translator are caught.
  @identical %{
    metadata: 0..9,
    offset_fetch: 1..6,
    find_coordinator: 0..3,
    join_group: 0..6,
    heartbeat: 0..4,
    leave_group: 0..4,
    sync_group: 0..4,
    describe_groups: 0..5,
    list_groups: 0..3,
    sasl_handshake: 0..1,
    api_versions: 0..3,
    create_topics: 2..5,
    delete_topics: 1..4,
    init_producer_id: 0..2,
    offset_for_leader_epoch: 2..3,
    add_offsets_to_txn: 0..1,
    txn_offset_commit: 0..2,
    sasl_authenticate: 0..1,
    create_delegation_token: 1..2,
    renew_delegation_token: 1..1,
    expire_delegation_token: 1..1,
    describe_delegation_token: 1..1,
    delete_groups: 0..2,
    incremental_alter_configs: 0..1,
    alter_partition_reassignments: 0..0,
    list_partition_reassignments: 0..0,
    offset_delete: 0..0
  }

  describe "cross-check against :kpro_schema" do
    test "every API in the table is defined by the JSON specs, kpro-sourced ones by both",
         %{spec: spec} do
      for api <- ApiTable.apis() do
        assert Map.has_key?(spec.apis, api), "#{api} missing from JSON specs"

        if ApiTable.kpro_max_version(api) != nil do
          assert :kpro_schema.vsn_range(api) != false, "#{api} missing from :kpro_schema"
        end
      end
    end

    test "API keys agree", %{spec: spec} do
      for api <- :kpro_schema.all_apis() do
        assert JsonSpec.api_key(spec, api) == :kpro_schema.api_key(api), "#{api}"
      end
    end

    test "flexible versions agree on every version kpro defines", %{spec: spec} do
      for api <- :kpro_schema.all_apis(),
          {lo, hi} = :kpro_schema.vsn_range(api),
          vsn <- lo..hi do
        kpro_flexible = vsn >= :kpro_schema.min_flexible_vsn(api)
        assert JsonSpec.flexible?(spec, api, vsn) == kpro_flexible, "#{api} v#{vsn}"
      end
    end

    # :kpro_schema has no named tagged fields (they are carried in the raw
    # tagged_fields bucket), so they are dropped from the JSON side before
    # comparing.  Where the JSON specs define them is pinned separately below.
    test "schemas are identical where pinned", %{spec: spec} do
      for {api, versions} <- @identical, vsn <- versions do
        assert drop_tagged(JsonSpec.request_schema(spec, api, vsn)) == :kpro_schema.req(api, vsn),
               "#{api} v#{vsn} request"

        assert drop_tagged(JsonSpec.response_schema(spec, api, vsn)) ==
                 :kpro_schema.rsp(api, vsn),
               "#{api} v#{vsn} response"
      end
    end

    test "the only named tagged fields in kpro-defined versions", %{spec: spec} do
      found =
        for api <- :kpro_schema.all_apis(),
            {lo, hi} = :kpro_schema.vsn_range(api),
            {json_lo, _} = JsonSpec.valid_versions(spec, api),
            vsn <- max(lo, json_lo)..hi//1,
            {dir, json} <- [
              request: JsonSpec.request_schema(spec, api, vsn),
              response: JsonSpec.response_schema(spec, api, vsn)
            ],
            path <- tagged_paths(json, []) do
          {api, vsn, dir, path}
        end

      # Kayrock keeps generating these versions from :kpro_schema, so these
      # stay in the raw tagged_fields bucket there (see Kayrock.Generate.ApiTable)
      assert found == [
               {:api_versions, 3, :response, [:supported_features]},
               {:api_versions, 3, :response, [:finalized_features_epoch]},
               {:api_versions, 3, :response, [:finalized_features]},
               {:api_versions, 3, :response, [:zk_migration_ready]},
               {:create_topics, 5, :response, [:topics, :topic_config_error_code]}
             ]
    end

    test "wire types agree for every version both sources define", %{spec: spec} do
      mismatches =
        for api <- :kpro_schema.all_apis(),
            {lo, hi} = :kpro_schema.vsn_range(api),
            {json_lo, _} = JsonSpec.valid_versions(spec, api),
            vsn <- max(lo, json_lo)..hi//1,
            {dir, kpro, json} <- [
              {:request, :kpro_schema.req(api, vsn), JsonSpec.request_schema(spec, api, vsn)},
              {:response, :kpro_schema.rsp(api, vsn), JsonSpec.response_schema(spec, api, vsn)}
            ],
            wire_types(normalize_kpro(api, vsn, dir, kpro)) != wire_types(drop_tagged(json)) do
          {api, vsn, dir}
        end

      assert mismatches == []
    end

    test "known :kpro_schema bug: Fetch v7+ forgotten_topics_data partitions", %{spec: spec} do
      # Kafka defines ForgottenTopicsData.Partitions as []int32; kpro_schema
      # (and therefore Kayrock's existing Fetch v7-v11 structs, which must not
      # change) models it as an array of partition structs.  Only an empty
      # forgotten_topics_data list encodes correctly with those versions.
      json = JsonSpec.request_schema(spec, :fetch, 7)

      assert {:array, [topic: :string, partitions: {:array, :int32}]} =
               json[:forgotten_topics_data]

      kpro = :kpro_schema.req(:fetch, 7)

      assert {:array, [topic: :string, partitions: {:array, [_ | _]}]} =
               kpro[:forgotten_topics_data]
    end
  end

  # kpro_schema nests some fields one level deeper than the JSON spec.  The
  # wire encoding of an inline (non-array) struct is just its fields, so
  # flattening these is wire-neutral.
  defp normalize_kpro(:fetch, vsn, :response, schema) when vsn >= 4 do
    update_in_array(schema, [:responses, :partition_responses], fn partition ->
      Keyword.fetch!(partition, :partition_header) ++ Keyword.delete(partition, :partition_header)
    end)
  end

  defp normalize_kpro(:fetch, vsn, :request, schema) when vsn >= 7 do
    # known kpro bug, see test above
    update_in_array(schema, [:forgotten_topics_data], fn topic ->
      Keyword.replace!(topic, :partitions, {:array, :int32})
    end)
  end

  defp normalize_kpro(:create_partitions, _vsn, :request, schema) do
    # kpro: new_partitions: [count, assignment: [[int32]]]
    # JSON: count, assignments: [%{broker_ids: [int32]}] (a single-field struct
    # encodes exactly like its only field)
    update_in_array(schema, [:topic_partitions], fn topic ->
      [count: :int32, assignment: {:array, {:array, :int32}}] =
        Keyword.fetch!(topic, :new_partitions)

      Keyword.delete(topic, :new_partitions) ++
        [count: :int32, assignment: {:array, [broker_ids: {:array, :int32}]}]
    end)
  end

  defp normalize_kpro(_api, _vsn, _dir, schema), do: schema

  defp update_in_array(schema, [key], fun) do
    Keyword.update!(schema, key, fn {array, elems} -> {array, fun.(elems)} end)
  end

  defp update_in_array(schema, [key | rest], fun) do
    Keyword.update!(schema, key, fn {array, elems} ->
      {array, update_in_array(elems, rest, fun)}
    end)
  end

  # remove named tagged fields (at any depth)
  defp drop_tagged(schema) when is_list(schema) do
    for {k, v} <- schema, not match?({:tagged_field, _}, v), do: {k, drop_tagged(v)}
  end

  defp drop_tagged({tag, inner}) when tag in [:array, :compact_array, :nullable_struct],
    do: {tag, drop_tagged(inner)}

  defp drop_tagged(type), do: type

  # paths to every named tagged field in a schema
  defp tagged_paths(schema, path) when is_list(schema) do
    Enum.flat_map(schema, fn
      {k, {:tagged_field, _}} -> [Enum.reverse([k | path])]
      {k, v} -> tagged_paths(v, [k | path])
    end)
  end

  defp tagged_paths({tag, inner}, path) when tag in [:array, :compact_array, :nullable_struct],
    do: tagged_paths(inner, path)

  defp tagged_paths(_type, _path), do: []

  # strip field names, leaving the wire layout
  defp wire_types(schema) when is_list(schema),
    do: Enum.map(schema, fn {_k, v} -> wire_types(v) end)

  defp wire_types({tag, inner}), do: {tag, wire_types(inner)}
  defp wire_types(type), do: type

  describe "translation rules" do
    test "a non-flexible version uses standard encodings and no tagged_fields", %{spec: spec} do
      assert JsonSpec.request_schema(spec, :metadata, 1) == [
               topics: {:array, [name: :string]}
             ]
    end

    test "flexible versions use compact encodings and add tagged_fields to every struct",
         %{spec: spec} do
      assert JsonSpec.request_schema(spec, :metadata, 9) == [
               topics: {:compact_array, [name: :compact_string, tagged_fields: :tagged_fields]},
               allow_auto_topic_creation: :boolean,
               include_cluster_authorized_operations: :boolean,
               include_topic_authorized_operations: :boolean,
               tagged_fields: :tagged_fields
             ]
    end

    test "uuid fields and nullableVersions", %{spec: spec} do
      # Metadata v10 adds TopicId (uuid) and makes Name nullable
      assert [topics: {:compact_array, topic}] ++ _ = JsonSpec.request_schema(spec, :metadata, 10)

      assert topic == [
               topic_id: :uuid,
               name: :compact_nullable_string,
               tagged_fields: :tagged_fields
             ]
    end

    test "fields are dropped outside their versions range", %{spec: spec} do
      # IncludeClusterAuthorizedOperations is versions 8-10
      assert Keyword.has_key?(
               JsonSpec.request_schema(spec, :metadata, 10),
               :include_cluster_authorized_operations
             )

      refute Keyword.has_key?(
               JsonSpec.request_schema(spec, :metadata, 11),
               :include_cluster_authorized_operations
             )
    end

    test "taggedVersions fields become named tagged fields, sorted by tag", %{spec: spec} do
      schema = JsonSpec.response_schema(spec, :api_versions, 3)

      assert [
               error_code: :int16,
               api_keys: {:compact_array, _},
               throttle_time_ms: :int32,
               supported_features: {:tagged_field, supported},
               finalized_features_epoch:
                 {:tagged_field, [tag: 1, type: :int64, default: -1, nullable: false]},
               finalized_features: {:tagged_field, [{:tag, 2} | _]},
               zk_migration_ready:
                 {:tagged_field, [tag: 3, type: :boolean, default: 0, nullable: false]},
               tagged_fields: :tagged_fields
             ] = schema

      assert supported == [
               tag: 0,
               type:
                 {:compact_array,
                  [
                    name: :compact_string,
                    min_version: :int16,
                    max_version: :int16,
                    tagged_fields: :tagged_fields
                  ]},
               default: [],
               nullable: false
             ]
    end

    test "tagged fields are always compact and are not inline", %{spec: spec} do
      # Fetch v12 request: ClusterId is a nullable tagged string, tag 0
      schema = JsonSpec.request_schema(spec, :fetch, 12)

      assert schema[:cluster_id] ==
               {:tagged_field,
                [tag: 0, type: :compact_nullable_string, default: nil, nullable: true]}

      # declared first in the JSON, but tagged fields go after the inline ones
      assert [{:replica_id, :int32} | _] = schema
      assert [{:cluster_id, _}, {:tagged_fields, :tagged_fields}] = Enum.take(schema, -2)
    end

    test "nested struct tagged fields", %{spec: spec} do
      # Fetch v12 response partitions carry three tagged structs
      schema = JsonSpec.response_schema(spec, :fetch, 12)
      {:compact_array, topic} = schema[:responses]
      {:compact_array, partition} = topic[:partitions]

      assert {:tagged_field, [tag: 0, type: diverging, default: nil, nullable: false]} =
               partition[:diverging_epoch]

      assert diverging == [epoch: :int32, end_offset: :int64, tagged_fields: :tagged_fields]
      assert {:tagged_field, [{:tag, 1} | _]} = partition[:current_leader]
      assert {:tagged_field, [{:tag, 2} | _]} = partition[:snapshot_id]

      assert [:current_leader, :diverging_epoch, :snapshot_id] --
               Keyword.keys(Enum.take(partition, -4)) == []
    end

    test "struct-typed tagged field inside a struct tagged field", %{spec: spec} do
      # Fetch v15 request: ReplicaState is a tagged struct (tag 1)
      schema = JsonSpec.request_schema(spec, :fetch, 15)

      assert {:tagged_field,
              [
                tag: 1,
                type: [replica_id: :int32, replica_epoch: :int64, tagged_fields: :tagged_fields],
                default: nil,
                nullable: false
              ]} = schema[:replica_state]

      refute Keyword.has_key?(schema, :replica_id)
    end

    test "defaults: hex and large integers, nullable arrays, null strings", %{spec: spec} do
      # FetchRequest v18 Partitions.HighWatermark default 9223372036854775807
      {:compact_array, topic} = JsonSpec.request_schema(spec, :fetch, 18)[:topics]
      {:compact_array, partition} = topic[:partitions]

      assert {:tagged_field,
              [tag: 1, type: :int64, default: 9_223_372_036_854_775_807, nullable: false]} =
               partition[:high_watermark]

      assert {:tagged_field, [tag: 0, type: :uuid, default: <<0::128>>, nullable: false]} =
               partition[:replica_directory_id]

      # BrokerHeartbeatRequest v2 CordonedLogDirs: nullable []uuid, default null
      schema = JsonSpec.request_schema(spec, :broker_heartbeat, 2)

      assert schema[:cordoned_log_dirs] ==
               {:tagged_field,
                [tag: 1, type: {:compact_array, :uuid}, default: nil, nullable: true]}

      assert schema[:offline_log_dirs] ==
               {:tagged_field,
                [tag: 0, type: {:compact_array, :uuid}, default: [], nullable: false]}
    end

    test "tagged_default/3 parses Kafka default literals" do
      field = fn default -> %{"name" => "F", "versions" => "0+", "default" => default} end

      assert JsonSpec.tagged_default(field.("0x7fffffff"), :int32, 0) == 0x7FFFFFFF
      assert JsonSpec.tagged_default(field.("-0x10"), :int32, 0) == -16
      assert JsonSpec.tagged_default(field.("-2147483648"), :int32, 0) == -2_147_483_648
      assert JsonSpec.tagged_default(field.(-1), :int32, 0) == -1
      assert JsonSpec.tagged_default(field.(nil), :int16, 0) == 0
      assert JsonSpec.tagged_default(field.("true"), :boolean, 0) == 1
      assert JsonSpec.tagged_default(field.(nil), :boolean, 0) == 0
      assert JsonSpec.tagged_default(field.("1.5"), :float64, 0) == 1.5
      assert JsonSpec.tagged_default(field.(nil), :float64, 0) == 0.0
      assert JsonSpec.tagged_default(field.(nil), :compact_string, 0) == ""
      assert JsonSpec.tagged_default(field.("abc"), :compact_string, 0) == "abc"
      assert JsonSpec.tagged_default(field.(nil), :uuid, 0) == <<0::128>>
      assert JsonSpec.tagged_default(field.(nil), {:compact_array, :int32}, 0) == []
      assert JsonSpec.tagged_default(field.(nil), [a: :int8], 0) == nil

      nullable = Map.put(field.("null"), "nullableVersions", "0+")
      assert JsonSpec.tagged_default(nullable, :compact_nullable_string, 0) == nil

      assert_raise ArgumentError, ~r/not nullable/, fn ->
        JsonSpec.tagged_default(field.("null"), :compact_string, 0)
      end

      assert_raise ArgumentError, ~r/Invalid integer default/, fn ->
        JsonSpec.tagged_default(field.("abc"), :int32, 0)
      end
    end

    test "tagged fields are rejected in non-flexible versions and must have unique tags" do
      message = fn fields ->
        JsonSpec.parse!(
          Jason.encode!(%{
            name: "TestRequest",
            type: "request",
            apiKey: 9999,
            validVersions: "0-1",
            flexibleVersions: "1+",
            fields: fields
          })
        )
      end

      tagged = fn name, tag ->
        %{name: name, type: "int32", versions: "0+", taggedVersions: "0+", tag: tag}
      end

      assert_raise ArgumentError, ~r/non-flexible version 0/, fn ->
        JsonSpec.message_schema(message.([tagged.("A", 0)]), 0)
      end

      assert_raise ArgumentError, ~r/Duplicate tags/, fn ->
        JsonSpec.message_schema(message.([tagged.("A", 0), tagged.("B", 0)]), 1)
      end

      assert_raise ArgumentError, ~r/no valid "tag"/, fn ->
        JsonSpec.message_schema(message.([Map.delete(tagged.("A", 0), :tag)]), 1)
      end

      assert JsonSpec.message_schema(message.([tagged.("B", 5), tagged.("A", 2)]), 1) == [
               a: {:tagged_field, [tag: 2, type: :int32, default: 0, nullable: false]},
               b: {:tagged_field, [tag: 5, type: :int32, default: 0, nullable: false]},
               tagged_fields: :tagged_fields
             ]
    end

    test "commonStructs are resolved, primitive arrays stay primitive", %{spec: spec} do
      schema = JsonSpec.request_schema(spec, :consumer_group_heartbeat, 0)

      assert schema[:topic_partitions] ==
               {:compact_array,
                [
                  topic_id: :uuid,
                  partitions: {:compact_array, :int32},
                  tagged_fields: :tagged_fields
                ]}

      assert schema[:subscribed_topic_names] == {:compact_array, :compact_string}
    end

    test "nullable struct fields become {:nullable_struct, schema}", %{spec: spec} do
      schema = JsonSpec.response_schema(spec, :consumer_group_heartbeat, 0)
      assert {:nullable_struct, assignment} = schema[:assignment]
      assert {:compact_array, [{:topic_id, :uuid} | _]} = assignment[:topic_partitions]
    end

    test "float64 fields", %{spec: spec} do
      [_, _, _, {:entries, {:compact_array, entry}} | _] =
        JsonSpec.response_schema(spec, :describe_client_quotas, 1)

      assert {:compact_array, [key: _, value: :float64, tagged_fields: _]} = entry[:values]
    end

    test "uint16 fields", %{spec: spec} do
      schema = JsonSpec.request_schema(spec, :broker_registration, 0)
      assert {:compact_array, listener} = schema[:listeners]
      assert listener[:port] == :uint16
    end

    test "flexible records are compact_records", %{spec: spec} do
      assert [{:transactional_id, _}, _, _, {:topic_data, {:compact_array, topic}} | _] =
               JsonSpec.request_schema(spec, :produce, 9)

      assert {:compact_array, partition} = topic[:partition_data]
      assert partition[:records] == :compact_records

      assert [{:transactional_id, _}, _, _, {:topic_data, {:array, old_topic}}] =
               JsonSpec.request_schema(spec, :produce, 8)

      assert {:array, old_partition} = old_topic[:partition_data]
      assert old_partition[:records] == :records
    end

    test "asking for an undefined version raises", %{spec: spec} do
      assert_raise ArgumentError, ~r/does not define version 0/, fn ->
        JsonSpec.request_schema(spec, :produce, 0)
      end
    end
  end

  describe "headers" do
    test "request header: v2 for flexible versions, v1 otherwise", %{spec: spec} do
      assert JsonSpec.request_header_version(spec, :metadata, 8) == 1
      assert JsonSpec.request_header_version(spec, :metadata, 9) == 2
      assert JsonSpec.request_header_version(spec, :api_versions, 3) == 2
    end

    test "response header: v1 for flexible versions, v0 otherwise", %{spec: spec} do
      assert JsonSpec.response_header_version(spec, :metadata, 8) == 0
      assert JsonSpec.response_header_version(spec, :metadata, 9) == 1
    end

    test "ApiVersions response always uses header v0 (KIP-511)", %{spec: spec} do
      for vsn <- 0..4, do: assert(JsonSpec.response_header_version(spec, :api_versions, vsn) == 0)
    end

    test "ClientId keeps the non-compact encoding in flexible header v2", %{spec: spec} do
      assert JsonSpec.message_schema(spec.request_header, 2) == [
               request_api_key: :int16,
               request_api_version: :int16,
               correlation_id: :int32,
               client_id: :nullable_string,
               tagged_fields: :tagged_fields
             ]
    end
  end

  describe "versions and stability" do
    test "parse_versions/1" do
      assert JsonSpec.parse_versions("none") == :none
      assert JsonSpec.parse_versions("3+") == {3, :infinity}
      assert JsonSpec.parse_versions("0-13") == {0, 13}
      assert JsonSpec.parse_versions("5") == {5, 5}
    end

    test "in_range?/2" do
      assert JsonSpec.in_range?("3+", 3)
      refute JsonSpec.in_range?("3+", 2)
      assert JsonSpec.in_range?("8-10", 10)
      refute JsonSpec.in_range?("8-10", 11)
      refute JsonSpec.in_range?("none", 0)
      refute JsonSpec.in_range?(nil, 0)
    end

    test "latestVersionUnstable excludes the top version", %{spec: spec} do
      # InitProducerIdRequest 4.4.0: validVersions 0-6, latestVersionUnstable: true
      assert {0, 6} = JsonSpec.valid_versions(spec, :init_producer_id)
      assert JsonSpec.max_stable_version(spec, :init_producer_id) == 5
      assert JsonSpec.max_stable_version(spec, :metadata) == 13
    end
  end

  describe "parsing" do
    test "strip_comments/1 removes line comments but not // inside strings" do
      json = ~s|// header\n{ "a": "http://x", // trailing\n "b": 1 }|
      assert Jason.decode!(JsonSpec.strip_comments(json)) == %{"a" => "http://x", "b" => 1}
    end

    test "strip_comments/1 handles escaped quotes in strings" do
      json = ~s|{ "a": "say \\"//hi\\"" } // c|
      assert Jason.decode!(JsonSpec.strip_comments(json)) == %{"a" => ~s|say "//hi"|}
    end

    test "every vendored spec parses", %{spec: spec} do
      # 95 request/response pairs in Kafka 4.4.0
      assert map_size(spec.apis) == 95
      assert spec.request_header && spec.response_header
    end

    test "every stable version of every API translates", %{spec: spec} do
      for api <- JsonSpec.all_apis(spec),
          # APIs removed by Kafka 4.x (KIP-896) declare validVersions "none"
          {lo, _} <- [JsonSpec.valid_versions(spec, api)],
          vsn <- lo..JsonSpec.max_stable_version(spec, api)//1 do
        assert is_list(JsonSpec.request_schema(spec, api, vsn))
        assert is_list(JsonSpec.response_schema(spec, api, vsn))
      end
    end

    test "api_name/1 and field_name/1" do
      assert JsonSpec.api_name("DescribeGroupsRequest") == :describe_groups
      assert JsonSpec.api_name("ApiVersionsResponse") == :api_versions
      assert JsonSpec.field_name("ThrottleTimeMs") == :throttle_time_ms
      assert JsonSpec.field_name("KRaftVersionFeature") == :k_raft_version_feature
    end
  end
end
