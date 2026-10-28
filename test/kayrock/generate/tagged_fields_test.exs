defmodule Kayrock.Generate.TaggedFieldsTest do
  @moduledoc """
  Named tagged fields (KIP-482) in generated code.

  Modules are generated with the real `Kayrock.Generate.build_all/2` from a
  test-only spec (`test/fixtures/kafka_messages/tagged_fields/`, API key
  32000) and from real Kafka 4.4.0 specs for versions Kayrock does not
  generate yet, compiled under a test namespace.  Nothing is added to
  `lib/generated`.

  Wire fixtures are written out by hand from the KIP-482 encoding:

      tagged section = uvarint(count) ++ for each field, ascending tag:
                       uvarint(tag) ++ uvarint(byte_size(value)) ++ value

  where `value` is the field's normal (compact) encoding.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.JsonSpec
  alias Kayrock.Test.GeneratedCode

  defmodule FixtureSchema do
    @moduledoc false
    alias Kayrock.Generate.JsonSpec

    @dir Path.expand("../../fixtures/kafka_messages/tagged_fields", __DIR__)

    defp spec, do: JsonSpec.load!(@dir)

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

  defmodule KafkaSchema do
    @moduledoc false
    # every stable version of an API, straight from the Kafka 4.4.0 specs
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

  @fixture Kayrock.Generate.TaggedFieldsTest.Fixture
  @kafka Kayrock.Generate.TaggedFieldsTest.Kafka

  setup_all do
    GeneratedCode.compile!(:tagged_fields_test, FixtureSchema, @fixture)

    for api <- [:api_versions, :fetch, :broker_heartbeat] do
      GeneratedCode.compile!(api, KafkaSchema, @kafka)
    end

    :ok
  end

  defp req_mod(vsn), do: GeneratedCode.module(@fixture, :tagged_fields_test, vsn, Request)
  defp rsp_mod(vsn), do: GeneratedCode.module(@fixture, :tagged_fields_test, vsn, Response)
  defp kafka_mod(api, vsn, kind), do: GeneratedCode.module(@kafka, api, vsn, kind)

  defp body(request), do: GeneratedCode.request_body(request)

  # minimal v1 request: every tagged field at its default
  defp base_request(fields \\ []) do
    struct(
      req_mod(1),
      Keyword.merge(
        [id: 7, name: "n", items: [%{key: "k"}], inline: %{x: 3}],
        fields
      )
    )
  end

  # inline bytes of base_request/1 (ascending wire order), parameterized by
  # the item/inline/root tag sections
  defp base_wire(opts \\ []) do
    IO.iodata_to_binary([
      # id, name "n"
      <<7::32, 2, "n">>,
      # items: 1 element {key "k", <item tags>}
      <<2, 2, "k">>,
      Keyword.get(opts, :item_tags, <<0>>),
      # inline {x: 3, <inline tags>}
      <<3::32>>,
      Keyword.get(opts, :inline_tags, <<0>>),
      # root tag section
      Keyword.get(opts, :root_tags, <<0>>)
    ])
  end

  describe "struct shape" do
    test "named tagged fields are struct keys with their JSON defaults" do
      request = struct(req_mod(1))

      assert request.note == nil
      assert request.count == -1
      assert request.flag == 0
      assert request.ids == []
      assert request.leader == nil
      assert request.dir_id == <<0::128>>
      assert request.big_tag == 0
      assert request.tagged_fields == []
    end

    test "the non-flexible version has neither tagged fields nor a tag buffer" do
      keys = req_mod(0) |> struct() |> Map.from_struct() |> Map.keys() |> Enum.sort()
      assert keys == [:client_id, :correlation_id, :id, :inline, :items, :name]

      request = struct(req_mod(0), id: 7, name: "n", items: [%{key: "k"}], inline: %{x: 3})
      assert body(request) == <<7::32, 0, 1, "n", 1::32, 0, 1, "k", 3::32>>
    end

    test "the schema keeps the tag metadata" do
      schema = req_mod(1).schema()
      assert {:tagged_field, opts} = schema[:count]
      assert opts[:tag] == 1
      assert opts[:default] == -1
      assert List.last(schema) == {:tagged_fields, :tagged_fields}
    end
  end

  describe "serialization" do
    test "zero tagged fields: defaults are not written" do
      assert body(base_request()) == base_wire()
    end

    test "nil means 'not set' for non-nullable tagged fields" do
      request = base_request(count: nil, flag: nil, ids: nil, dir_id: nil, big_tag: nil)
      assert body(request) == base_wire()
    end

    test "a missing key is treated as not set" do
      request = Map.delete(base_request(), :count)
      assert body(request) == base_wire()
    end

    test "one tagged field" do
      # count (tag 1) = 5: tag 1, size 4, int32 5
      assert body(base_request(count: 5)) == base_wire(root_tags: <<1, 1, 4, 5::32>>)
    end

    test "several tagged fields are written in ascending tag order" do
      request =
        base_request(
          big_tag: 1,
          dir_id: <<0xAB::128>>,
          ids: [1, 2],
          flag: true,
          count: 0,
          note: "hi"
        )

      root_tags =
        IO.iodata_to_binary([
          # 6 fields
          <<6>>,
          # tag 0 note "hi": compact nullable string <<3, "hi">>
          <<0, 3, 3, "hi">>,
          # tag 1 count 0 (default is -1, so 0 is written)
          <<1, 4, 0::32>>,
          # tag 2 flag true
          <<2, 1, 1>>,
          # tag 3 ids [1, 2]: compact array <<3, 1::32, 2::32>>
          <<3, 9, 3, 1::32, 2::32>>,
          # tag 5 dir_id
          <<5, 16, 0xAB::128>>,
          # tag 200 (uvarint <<0xC8, 0x01>>) big_tag int64 1
          <<0xC8, 0x01, 8, 1::64>>
        ])

      assert body(request) == base_wire(root_tags: root_tags)
    end

    test "nullable tagged string: nil is the default, empty string is a value" do
      assert body(base_request(note: nil)) == base_wire()
      assert body(base_request(note: "")) == base_wire(root_tags: <<1, 0, 1, 1>>)
    end

    test "booleans: false is the default, integer 1/0 are accepted" do
      assert body(base_request(flag: false)) == base_wire()
      assert body(base_request(flag: 0)) == base_wire()
      assert body(base_request(flag: 1)) == base_wire(root_tags: <<1, 2, 1, 1>>)
    end

    test "empty array equals the default and is not written" do
      assert body(base_request(ids: [])) == base_wire()
    end

    test "struct tagged field with its own nested tagged field" do
      # leader (tag 4) = {leader_id: 9, info: "x"}
      # value = int32 9 ++ leader tag section {1 field, tag 0, size 2, <<2, "x">>}
      value = <<9::32, 1, 0, 2, 2, "x">>
      request = base_request(leader: %{leader_id: 9, info: "x"})

      assert body(request) ==
               base_wire(root_tags: IO.iodata_to_binary([<<1, 4, byte_size(value)>>, value]))

      # nested tagged field at its default: an empty tag section
      request = base_request(leader: %{leader_id: 9})
      assert body(request) == base_wire(root_tags: <<1, 4, 5, 9::32, 0>>)
    end

    test "tagged fields in array elements and inline structs" do
      request =
        base_request(
          items: [%{key: "k", weight: 2, blob: <<1, 2>>}, %{key: "k"}],
          inline: %{x: 3, y: 1}
        )

      assert body(request) ==
               IO.iodata_to_binary([
                 <<7::32, 2, "n">>,
                 # items: 2 elements
                 <<3>>,
                 # {key "k", tags: weight (0) = 2, blob (1) = <<1, 2>>}
                 <<2, "k", 2, 0, 4, 2::32, 1, 3, 3, 1, 2>>,
                 # {key "k", no tags}
                 <<2, "k", 0>>,
                 # inline {x: 3, tags: y (7) = 1 (default is 0x7fff)}
                 <<3::32, 1, 7, 2, 1::16>>,
                 <<0>>
               ])
    end

    test "unknown tags in tagged_fields are merged in tag order" do
      request = base_request(count: 5, tagged_fields: [{300, <<0xFF>>}, {6, <<>>}])

      assert body(request) ==
               base_wire(root_tags: <<3, 1, 4, 5::32, 6, 0, 0xAC, 0x02, 1, 0xFF>>)
    end

    test "an unknown tag clashing with a named field raises" do
      request = base_request(count: 5, tagged_fields: [{1, <<0::32>>}])

      assert_raise ArgumentError, ~r/also named fields/, fn -> body(request) end
    end
  end

  describe "deserialization" do
    # response v1 = header v1 (correlation id, tag buffer), error_code, then
    # the same layout as the request
    defp response_wire(opts \\ []) do
      IO.iodata_to_binary([<<99::32, 0, 0::16>>, base_wire(opts)])
    end

    test "zero tagged fields: named fields take their defaults" do
      assert {response, <<>>} = rsp_mod(1).deserialize(response_wire())

      assert %{
               correlation_id: 99,
               error_code: 0,
               id: 7,
               name: "n",
               note: nil,
               count: -1,
               flag: 0,
               ids: [],
               leader: nil,
               dir_id: <<0::128>>,
               big_tag: 0,
               tagged_fields: [],
               items: [%{key: "k", weight: 1, blob: nil, tagged_fields: []}],
               inline: %{x: 3, y: 0x7FFF, tagged_fields: []}
             } = response
    end

    test "known tags decode into named fields; unknown tags are kept raw" do
      root_tags =
        IO.iodata_to_binary([
          <<5>>,
          <<0, 3, 3, "hi">>,
          <<2, 1, 1>>,
          # tag 4 leader {leader_id 9, tags: 1 unknown tag 8 of size 0}
          <<4, 7, 9::32, 1, 8, 0>>,
          # unknown tags 6 and 300
          <<6, 2, 0xDE, 0xAD>>,
          <<0xAC, 0x02, 0>>
        ])

      assert {response, <<>>} = rsp_mod(1).deserialize(response_wire(root_tags: root_tags))
      assert response.note == "hi"
      assert response.flag == 1
      assert response.leader == %{leader_id: 9, info: nil, tagged_fields: [{8, <<>>}]}
      assert response.count == -1
      assert response.tagged_fields == [{6, <<0xDE, 0xAD>>}, {300, <<>>}]
    end

    test "tags may arrive out of order and are decoded the same" do
      root_tags = <<2, 3, 9, 3, 1::32, 2::32, 1, 4, 5::32>>

      assert {response, <<>>} = rsp_mod(1).deserialize(response_wire(root_tags: root_tags))
      assert response.ids == [1, 2]
      assert response.count == 5
    end

    test "nested tagged fields in arrays and inline structs" do
      wire =
        IO.iodata_to_binary([
          <<99::32, 0, 0::16, 7::32, 2, "n">>,
          <<3>>,
          <<2, "k", 2, 0, 4, 2::32, 1, 3, 3, 1, 2>>,
          # unknown tag 9 in the second element
          <<2, "k", 1, 9, 1, 0xEE>>,
          <<3::32, 1, 7, 2, 1::16>>,
          <<0>>
        ])

      assert {response, <<>>} = rsp_mod(1).deserialize(wire)

      assert response.items == [
               %{key: "k", weight: 2, blob: <<1, 2>>, tagged_fields: []},
               %{key: "k", weight: 1, blob: nil, tagged_fields: [{9, <<0xEE>>}]}
             ]

      assert response.inline == %{x: 3, y: 1, tagged_fields: []}
    end

    test "non-flexible version" do
      wire = <<99::32, 0::16, 7::32, 0, 1, "n", 1::32, 0, 1, "k", 3::32>>

      assert {response, <<>>} = rsp_mod(0).deserialize(wire)
      assert response.items == [%{key: "k"}]
      assert response.inline == %{x: 3}
      refute Map.has_key?(response, :count)
    end
  end

  describe "round trips" do
    # serialize a request, then decode the body with the response module
    # (whose layout is the request's plus a leading error_code)
    defp round_trip(request) do
      wire = IO.iodata_to_binary([<<1::32, 0, 0::16>>, body(request)])
      {decoded, <<>>} = rsp_mod(1).deserialize(wire)
      decoded
    end

    test "every named tagged field" do
      request =
        base_request(
          note: "note",
          count: 0,
          flag: true,
          ids: [3, 1],
          leader: %{leader_id: 4, info: "i"},
          dir_id: <<5::128>>,
          big_tag: -1,
          items: [%{key: "a", weight: -5, blob: <<>>}],
          inline: %{x: 1, y: -1},
          tagged_fields: [{1000, <<1, 2, 3>>}]
        )

      decoded = round_trip(request)

      assert decoded.note == "note"
      assert decoded.count == 0
      assert decoded.flag == 1
      assert decoded.ids == [3, 1]
      assert decoded.leader == %{leader_id: 4, info: "i", tagged_fields: []}
      assert decoded.dir_id == <<5::128>>
      assert decoded.big_tag == -1
      assert decoded.items == [%{key: "a", weight: -5, blob: <<>>, tagged_fields: []}]
      assert decoded.inline == %{x: 1, y: -1, tagged_fields: []}
      assert decoded.tagged_fields == [{1000, <<1, 2, 3>>}]
    end

    test "decoded values (including integer booleans) re-encode identically" do
      request = base_request(flag: true, count: 2, tagged_fields: [{50, <<9>>}])
      decoded = round_trip(request)

      re_encoded =
        req_mod(1)
        |> struct(decoded |> Map.from_struct() |> Map.drop([:error_code]))
        |> body()

      assert re_encoded == body(request)
    end
  end

  describe "real Kafka 4.4.0 specs (versions Kayrock does not generate yet)" do
    test "ApiVersions v4 response decodes SupportedFeatures and FinalizedFeaturesEpoch" do
      # ApiVersions responses always use header v0
      wire =
        IO.iodata_to_binary([
          <<5::32>>,
          # error_code, api_keys: 1 {18, 0, 4, tags}, throttle_time_ms
          <<0::16, 2, 18::16, 0::16, 4::16, 0, 0::32>>,
          # 2 tagged fields
          <<2>>,
          # tag 0 SupportedFeatures: [{"f", 0, 1, tags}] = <<2, 2, "f", 0::16, 1::16, 0>>
          <<0, 8, 2, 2, "f", 0::16, 1::16, 0>>,
          # tag 1 FinalizedFeaturesEpoch 42
          <<1, 8, 42::64>>
        ])

      assert {response, <<>>} = kafka_mod(:api_versions, 4, Response).deserialize(wire)

      assert response.supported_features == [
               %{name: "f", min_version: 0, max_version: 1, tagged_fields: []}
             ]

      assert response.finalized_features_epoch == 42
      assert response.finalized_features == []
      assert response.zk_migration_ready == 0
      assert response.tagged_fields == []
    end

    test "Fetch v12 request with a tagged ClusterId" do
      request =
        struct(kafka_mod(:fetch, 12, Request),
          replica_id: -1,
          max_wait_ms: 100,
          min_bytes: 1,
          max_bytes: 1024,
          isolation_level: 0,
          session_id: 0,
          session_epoch: -1,
          topics: [],
          forgotten_topics_data: [],
          rack_id: "",
          cluster_id: "c1"
        )

      assert body(request) ==
               IO.iodata_to_binary([
                 <<-1::32, 100::32, 1::32, 1024::32, 0, 0::32, -1::32>>,
                 # topics, forgotten_topics_data: empty; rack_id ""
                 <<1, 1, 1>>,
                 # 1 tagged field: tag 0, size 3, compact string "c1"
                 <<1, 0, 3, 3, "c1">>
               ])

      # nil is the default: an empty root tag section
      assert body(%{request | cluster_id: nil}) ==
               <<-1::32, 100::32, 1::32, 1024::32, 0, 0::32, -1::32, 1, 1, 1, 0>>
    end

    test "Fetch v12 response decodes nested tagged structs per partition" do
      wire =
        IO.iodata_to_binary([
          # header v1
          <<1::32, 0>>,
          # throttle_time_ms, error_code, session_id
          <<0::32, 0::16, 0::32>>,
          # responses: 1 {topic "t", partitions: 1 {...}}
          <<2, 2, "t", 2>>,
          # partition_index, error_code, high_watermark, last_stable_offset,
          # log_start_offset
          <<0::32, 0::16, 10::64, 10::64, 0::64>>,
          # aborted_transactions: null, preferred_read_replica, records: null
          <<0, -1::32, 0>>,
          # partition tags: CurrentLeader (tag 1) = {leader_id 2, leader_epoch 5, tags}
          <<1, 1, 9, 2::32, 5::32, 0>>,
          # topic and root tag buffers
          <<0, 0>>
        ])

      assert {response, <<>>} = kafka_mod(:fetch, 12, Response).deserialize(wire)
      [%{partitions: [partition]}] = response.responses

      assert partition.current_leader == %{leader_id: 2, leader_epoch: 5, tagged_fields: []}
      assert partition.diverging_epoch == nil
      assert partition.snapshot_id == nil
      assert partition.tagged_fields == []
    end

    test "BrokerHeartbeat v2: nullable tagged uuid array (null default) vs non-nullable" do
      request =
        struct(kafka_mod(:broker_heartbeat, 2, Request),
          broker_id: 1,
          broker_epoch: 2,
          current_metadata_offset: 3,
          want_fence: false,
          want_shut_down: false
        )

      assert request.cordoned_log_dirs == nil
      assert request.offline_log_dirs == []

      base = <<1::32, 2::64, 3::64, 0, 0>>
      assert body(request) == base <> <<0>>

      # an empty list differs from the null default, so it is written
      assert body(%{request | cordoned_log_dirs: []}) == base <> <<1, 1, 1, 1>>

      assert body(%{request | offline_log_dirs: [<<1::128>>]}) ==
               base <> <<1, 0, 17, 2, 1::128>>
    end
  end

  test "the fixture API is not a real Kafka API" do
    refute :tagged_fields_test in JsonSpec.all_apis(JsonSpec.load!(ApiTable.spec_dir()))
  end
end
