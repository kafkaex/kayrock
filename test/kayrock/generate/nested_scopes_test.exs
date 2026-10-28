defmodule Kayrock.Generate.NestedScopesTest do
  @moduledoc """
  Nested structs that share a field name in one response.

  The generated response decoder keys the `deserialize_field/4` clauses of a
  nested struct on a scope atom, normally the struct's field name. When two
  nested structs with different schemas share a field name (Kafka 4.4.0
  StreamsGroupDescribe v1: `topology.subtopologies` and
  `topology_description.subtopologies`), the generator scopes them by path
  instead.

  It also checks the type of a required (non-nullable) top-level struct
  field (first used by StreamsGroupTopologyDescriptionUpdate v0): the
  struct defaults it to nil, so its type admits nil.

  These tests use a test-only schema so the generator is checked
  independently of any real API; nothing is added to `lib/generated`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Test.GeneratedCode

  defmodule Schema do
    @moduledoc false
    # A kpro_schema-shaped module for one test-only flexible API.

    # both start with `id` but continue differently, like the two
    # `subtopologies` structs (`subtopology_id`, then different fields)
    @shared_a [id: :int8, x: :int32, tagged_fields: :tagged_fields]
    @shared_b [id: :int8, y: :compact_string, z: :int8, tagged_fields: :tagged_fields]

    def vsn_range(:nested_scopes_test), do: {0, 1}
    def min_flexible_vsn(:nested_scopes_test), do: 0
    def api_key(:nested_scopes_test), do: 32_001
    def req(:nested_scopes_test, _vsn), do: [tagged_fields: :tagged_fields]

    # v0: `items` under two different parents, with different schemas
    def rsp(:nested_scopes_test, 0) do
      [
        first:
          {:nullable_struct, [items: {:compact_array, @shared_a}, tagged_fields: :tagged_fields]},
        second: [items: {:compact_array, @shared_b}, tagged_fields: :tagged_fields],
        tagged_fields: :tagged_fields
      ]
    end

    # v1: no duplicate names
    def rsp(:nested_scopes_test, 1) do
      [
        first: {:compact_array, @shared_a},
        second: [inner: @shared_b, tagged_fields: :tagged_fields],
        tagged_fields: :tagged_fields
      ]
    end
  end

  @namespace Kayrock.Generate.NestedScopesTest.Gen

  setup_all do
    GeneratedCode.compile!(:nested_scopes_test, Schema, @namespace)
    :ok
  end

  defp rsp_mod(vsn), do: GeneratedCode.module(@namespace, :nested_scopes_test, vsn, Response)

  test "structs sharing a field name decode with their own schemas" do
    wire =
      IO.iodata_to_binary([
        # correlation id + response header v1 tag buffer
        <<1::32, 0>>,
        # first: present; items [{id: 1, x: 7}]; first tag buffer
        <<1, 2, 1, 7::32, 0, 0>>,
        # second.items [{id: 2, y: "ab", z: 3}]; second tag buffer
        <<2, 2, 3, "ab", 3, 0, 0>>,
        # root tag buffer
        <<0>>
      ])

    assert {decoded, <<>>} = rsp_mod(0).deserialize(wire)
    assert decoded.first == %{items: [%{id: 1, x: 7, tagged_fields: []}], tagged_fields: []}

    assert decoded.second == %{
             items: [%{id: 2, y: "ab", z: 3, tagged_fields: []}],
             tagged_fields: []
           }
  end

  test "duplicated names get path scopes; unique names keep their field-name scope" do
    v0 = generated_source(0)
    assert v0 =~ ~s{deserialize_field(:"first.items", :id,}
    assert v0 =~ ~s{deserialize_field(:"second.items", :id,}
    refute v0 =~ "deserialize_field(:items,"
    assert v0 =~ "deserialize_field(:first, :items,"

    v1 = generated_source(1)
    refute v1 =~ ~s{deserialize_field(:"}
    assert v1 =~ "deserialize_field(:first, :id,"
    assert v1 =~ "deserialize_field(:inner, :id,"
  end

  test "a required top-level struct field defaults to nil and its type admits nil" do
    v1 = generated_source(1)
    assert v1 =~ "defstruct first: [], second: nil,"
    assert v1 =~ ~r/second:\s+nil\s+\|\s+%\{\s+inner: %\{/
  end

  defp generated_source(vsn) do
    :nested_scopes_test
    |> Kayrock.Generate.build_all(Schema)
    |> Macro.to_string()
    |> String.split("defmodule V")
    |> Enum.find(&String.starts_with?(&1, "#{vsn}.Response"))
  end
end
