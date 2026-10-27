defmodule Kayrock.Generate.JsonGeneratedCodeTest do
  @moduledoc """
  End-to-end tests of the JSON-driven generator.

  Modules are generated (with the real `Kayrock.Generate.build_all/2`) for APIs
  and versions that are *not* in `Kayrock.Generate.ApiTable`, so nothing is
  added to `lib/generated`.  They are compiled under a test-only namespace and
  wire data is round-tripped through them.  This exercises the generator paths
  only JSON specs need: `uuid`, `uint16`, `float64`, nullable structs,
  `compact_records` and compact arrays of fixed-width types.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.JsonSpec

  defmodule JsonOnlySchema do
    @moduledoc false
    # A kpro_schema-shaped module that serves every stable version of an API
    # straight from the JSON specs.
    alias Kayrock.Generate.ApiTable
    alias Kayrock.Generate.JsonSpec

    defp spec, do: JsonSpec.load!(ApiTable.spec_dir())

    def vsn_range(api) do
      {min, _} = JsonSpec.valid_versions(spec(), api)
      {min, JsonSpec.max_stable_version(spec(), api)}
    end

    def min_flexible_vsn(api), do: JsonSpec.min_flexible_version(spec(), api) || 9999
    def api_key(api), do: JsonSpec.api_key(spec(), api)
    def req(api, vsn), do: JsonSpec.request_schema(spec(), api, vsn)
    def rsp(api, vsn), do: JsonSpec.response_schema(spec(), api, vsn)
    def request_header_vsn(api, vsn), do: JsonSpec.request_header_version(spec(), api, vsn)
    def response_header_vsn(api, vsn), do: JsonSpec.response_header_version(spec(), api, vsn)
  end

  alias Kayrock.Test.GeneratedCode

  @namespace Kayrock.Generate.JsonGeneratedCodeTest.Gen

  setup_all do
    for api <- [
          :consumer_group_heartbeat,
          :describe_client_quotas,
          :broker_registration,
          :describe_topic_partitions,
          :produce
        ] do
      GeneratedCode.compile!(api, JsonOnlySchema, @namespace)
    end

    :ok
  end

  defp mod(api, vsn, kind), do: GeneratedCode.module(@namespace, api, vsn, kind)

  defp request_body(request), do: GeneratedCode.request_body(request)

  test "generated modules expose the Kafka API key and version" do
    req = mod(:consumer_group_heartbeat, 1, Request)
    assert req.api_key() == 68
    assert req.api_vsn() == 1
    assert {:compact_array, _} = Keyword.fetch!(req.schema(), :topic_partitions)
  end

  describe "consumer_group_heartbeat (uuid, nullable struct, compact arrays)" do
    test "request serializes uuids, nullable strings and compact arrays" do
      topic_id = <<1::128>>

      request =
        struct(mod(:consumer_group_heartbeat, 0, Request),
          group_id: "g",
          member_id: "m",
          member_epoch: 0,
          instance_id: nil,
          rack_id: nil,
          rebalance_timeout_ms: 30_000,
          subscribed_topic_names: ["t"],
          server_assignor: nil,
          topic_partitions: [%{topic_id: topic_id, partitions: [0]}]
        )

      assert request_body(request) ==
               IO.iodata_to_binary([
                 # group_id, member_id
                 <<2, "g">>,
                 <<2, "m">>,
                 # member_epoch
                 <<0::32>>,
                 # instance_id, rack_id: null
                 <<0, 0>>,
                 <<30_000::32>>,
                 # subscribed_topic_names ["t"]
                 <<2, 2, "t">>,
                 # server_assignor: null
                 <<0>>,
                 # topic_partitions: 1 element
                 <<2>>,
                 topic_id,
                 <<2, 0::32>>,
                 # element tag buffer, root tag buffer
                 <<0, 0>>
               ])
    end

    test "a nil uuid serializes as the zero UUID" do
      request =
        struct(mod(:consumer_group_heartbeat, 0, Request),
          group_id: "g",
          member_id: "m",
          member_epoch: 0,
          rebalance_timeout_ms: 0,
          subscribed_topic_names: nil,
          topic_partitions: [%{topic_id: nil, partitions: []}]
        )

      assert request_body(request) ==
               IO.iodata_to_binary([
                 <<2, "g", 2, "m", 0::32>>,
                 # instance_id, rack_id: null; rebalance_timeout_ms
                 <<0, 0, 0::32>>,
                 # subscribed_topic_names, server_assignor: null
                 <<0, 0>>,
                 # topic_partitions: [{zero uuid, []}]
                 <<2, 0::128, 1, 0>>,
                 <<0>>
               ])
    end

    test "response with an assignment round-trips" do
      topic_id = :crypto.strong_rand_bytes(16)

      wire =
        IO.iodata_to_binary([
          # correlation id + response header v1 tag buffer
          <<7::32, 0>>,
          # throttle_time_ms, error_code, error_message: null
          <<0::32, 0::16, 0>>,
          # member_id "m1"
          <<3, "m1">>,
          # member_epoch, heartbeat_interval_ms
          <<5::32, 3000::32>>,
          # assignment: present
          <<1>>,
          # assignment.topic_partitions: 1 element
          <<2>>,
          topic_id,
          # partitions [0, 2]
          <<3, 0::32, 2::32>>,
          # element tag buffer, assignment tag buffer, root tag buffer
          <<0, 0, 0>>
        ])

      assert {decoded, <<>>} = mod(:consumer_group_heartbeat, 1, Response).deserialize(wire)
      assert decoded.correlation_id == 7
      assert decoded.member_id == "m1"
      assert decoded.error_message == nil
      assert decoded.member_epoch == 5

      assert %{topic_partitions: [%{topic_id: ^topic_id, partitions: [0, 2]}], tagged_fields: []} =
               decoded.assignment
    end

    test "response with a null assignment" do
      wire = <<7::32, 0, 0::32, 0::16, 0, 3, "m1", 5::32, 3000::32, -1::8-signed, 0>>

      assert {%{assignment: nil, tagged_fields: []}, <<>>} =
               mod(:consumer_group_heartbeat, 1, Response).deserialize(wire)
    end
  end

  describe "describe_topic_partitions (nullable struct in a request)" do
    test "present struct is int8 1 followed by its fields" do
      request =
        struct(mod(:describe_topic_partitions, 0, Request),
          topics: [%{name: "t"}],
          response_partition_limit: 2000,
          cursor: %{topic_name: "t", partition_index: 3}
        )

      assert request_body(request) ==
               <<2, 2, "t", 0, 2000::32, 1, 2, "t", 3::32, 0, 0>>
    end

    test "nil struct is int8 -1" do
      request =
        struct(mod(:describe_topic_partitions, 0, Request),
          topics: [],
          response_partition_limit: 1,
          cursor: nil
        )

      assert request_body(request) == <<1, 1::32, -1::8-signed, 0>>
    end
  end

  describe "describe_client_quotas (float64)" do
    test "response float64 values decode" do
      wire =
        IO.iodata_to_binary([
          <<1::32, 0>>,
          # throttle_time_ms, error_code, error_message: null
          <<0::32, 0::16, 0>>,
          # entries: 1
          <<2>>,
          # entity: 1 element {entity_type "user", entity_name null, tags}
          <<2, 5, "user", 0, 0>>,
          # values: 1 element {key "k", value 1.5, tags}
          <<2, 2, "k", 1.5::64-float, 0>>,
          # entry tag buffer, root tag buffer
          <<0, 0>>
        ])

      assert {decoded, <<>>} = mod(:describe_client_quotas, 1, Response).deserialize(wire)

      assert [
               %{
                 entity: [%{entity_type: "user", entity_name: nil}],
                 values: [%{key: "k", value: 1.5}]
               }
             ] = decoded.entries
    end
  end

  describe "broker_registration (uint16)" do
    test "uint16 serializes unsigned" do
      req_mod = mod(:broker_registration, 0, Request)
      assert {:compact_array, listener} = Keyword.fetch!(req_mod.schema(), :listeners)
      assert Keyword.fetch!(listener, :port) == :uint16

      request =
        struct(req_mod,
          broker_id: 1,
          cluster_id: "c",
          incarnation_id: <<9::128>>,
          listeners: [%{name: "PLAINTEXT", host: "h", port: 65_535, security_protocol: 0}],
          features: [],
          rack: nil
        )

      assert IO.iodata_to_binary([
               <<1::32>>,
               <<2, "c">>,
               <<9::128>>,
               <<2, 10, "PLAINTEXT", 2, "h", 0xFF, 0xFF, 0::16, 0>>,
               <<1, 0, 0>>
             ]) == request_body(request)
    end
  end

  describe "produce v9 (compact_records)" do
    test "request records use an unsigned-varint length prefix" do
      batch = %Kayrock.RecordBatch{
        records: [%Kayrock.RecordBatch.Record{key: "k", value: "v", timestamp: 1, offset: 0}]
      }

      request =
        struct(mod(:produce, 9, Request),
          transactional_id: nil,
          acks: 1,
          timeout_ms: 1000,
          topic_data: [%{name: "t", partition_data: [%{index: 0, records: batch}]}]
        )

      <<size::32, batch_bin::binary>> = IO.iodata_to_binary(Kayrock.RecordBatch.serialize(batch))
      assert size == byte_size(batch_bin)

      assert request_body(request) ==
               IO.iodata_to_binary([
                 # transactional_id: null, acks, timeout_ms
                 <<0, 1::16, 1000::32>>,
                 # topic_data: 1 {name "t", partition_data: 1 {index 0, records}}
                 <<2, 2, "t", 2, 0::32>>,
                 Kayrock.Serialize.encode_unsigned_varint(size + 1),
                 batch_bin,
                 # partition, topic and root tag buffers
                 <<0, 0, 0>>
               ])
    end

    test "null records serialize as varint 0" do
      request =
        struct(mod(:produce, 9, Request),
          transactional_id: nil,
          acks: 1,
          timeout_ms: 1000,
          topic_data: [%{name: "t", partition_data: [%{index: 0, records: nil}]}]
        )

      assert request_body(request) == <<0, 1::16, 1000::32, 2, 2, "t", 2, 0::32, 0, 0, 0, 0>>
    end
  end

  test "JSON-only test APIs are not part of the generated table" do
    spec = JsonSpec.load!(ApiTable.spec_dir())

    for api <- [
          :consumer_group_heartbeat,
          :describe_client_quotas,
          :broker_registration,
          :describe_topic_partitions
        ] do
      assert Map.has_key?(spec.apis, api)
      refute api in ApiTable.apis()
    end

    assert ApiTable.max_version(:produce) < 9
  end
end
