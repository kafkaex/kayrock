defmodule Kayrock.CreatePartitionsJsonVersionsTest do
  @moduledoc """
  CreatePartitions (key 37) v2-v3, generated from the Kafka 4.4.0 JSON specs.

  * v2: first flexible version (compact encodings, tag buffers)
  * v3: same schema as v2 (may return THROTTLING_QUOTA_EXCEEDED, KIP-599)

  The JSON field names differ from the kpro-sourced v0-v1 structs
  (`topics`/`count`/`assignments`/`results` instead of
  `topic_partitions`/`new_partitions`/`topic_errors`).
  Fixtures are derived by hand from `CreatePartitions{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.CreatePartitions

  test "v0-v3 are generated" do
    assert CreatePartitions.min_vsn() == 0
    assert CreatePartitions.max_vsn() == 3
  end

  for version <- [2, 3] do
    test "v#{version} request serializes per the JSON spec" do
      version = unquote(version)

      request =
        struct(Module.concat([CreatePartitions, :"V#{version}", Request]),
          correlation_id: version,
          client_id: "kayrock",
          topics: [
            %{name: "t1", count: 4, assignments: [%{broker_ids: [1]}]},
            %{name: "t2", count: 2, assignments: nil}
          ],
          timeout_ms: 5_000,
          validate_only: false
        )

      assert serialize(request) ==
               request_header(37, version, version, "kayrock") <>
                 carray([
                   cstr("t1") <> <<4::32>> <> carray([carray([<<1::32>>]) <> tags()]) <> tags(),
                   cstr("t2") <> <<2::32>> <> carray(nil) <> tags()
                 ]) <> <<5_000::32, 0>> <> tags()
    end

    test "v#{version} response decodes per the JSON spec" do
      version = unquote(version)
      module = Module.concat([CreatePartitions, :"V#{version}", Response])

      wire =
        response_header(version) <>
          <<7::32>> <>
          carray([
            cstr("t1") <> <<0::16>> <> cstr(nil) <> tags(),
            cstr("t2") <> <<37::16>> <> cstr("bad count") <> tags()
          ]) <> tags()

      assert {response, <<>>} = module.deserialize(wire)

      assert response == %{
               __struct__: module,
               correlation_id: version,
               throttle_time_ms: 7,
               results: [
                 %{name: "t1", error_code: 0, error_message: nil, tagged_fields: []},
                 %{name: "t2", error_code: 37, error_message: "bad count", tagged_fields: []}
               ],
               tagged_fields: []
             }

      assert CreatePartitions.deserialize(version, wire) == {response, <<>>}
    end
  end
end
