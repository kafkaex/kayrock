defmodule Kayrock.Generate.SchemaSourceTest do
  @moduledoc """
  Tests for the generator's version-source policy (`Kayrock.Generate.ApiTable`
  and `Kayrock.Generate.SchemaSource`).
  """
  use ExUnit.Case, async: true

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.SchemaSource

  test "the table validates against :kpro_schema and the JSON specs" do
    assert SchemaSource.validate!() == :ok
  end

  test "the allowlist matches the generated Kayrock.KafkaSchemaMetadata" do
    assert ApiTable.apis() == Kayrock.KafkaSchemaMetadata.all_apis()

    for {api, max} <- ApiTable.max_versions() do
      assert {_min, ^max} = Kayrock.KafkaSchemaMetadata.version_range(api)
    end
  end

  test "versions up to the frozen kpro maxima come from :kpro_schema" do
    for {api, kpro_max} <- ApiTable.kpro_max_versions() do
      {min, _} = SchemaSource.vsn_range(api)

      for vsn <- min..kpro_max do
        assert SchemaSource.source(api, vsn) == :kpro
        assert SchemaSource.req(api, vsn) == :kpro_schema.req(api, vsn)
        assert SchemaSource.rsp(api, vsn) == :kpro_schema.rsp(api, vsn)
      end
    end
  end

  test "kpro-sourced APIs keep kpro_schema's min version and generate at least its max" do
    for {api, kpro_max} <- ApiTable.kpro_max_versions() do
      {kpro_min, ^kpro_max} = :kpro_schema.vsn_range(api)
      {min, max} = SchemaSource.vsn_range(api)
      assert min == kpro_min, "#{api}"
      assert max >= kpro_max, "#{api}"
    end
  end

  test "JSON-only APIs start at the JSON specs' lowest valid version" do
    spec = SchemaSource.json_spec()

    for api <- ApiTable.apis(), ApiTable.kpro_max_version(api) == nil do
      {json_min, _} = Kayrock.Generate.JsonSpec.valid_versions(spec, api)
      assert {^json_min, _} = SchemaSource.vsn_range(api), "#{api}"
    end
  end

  test "versions outside the table raise" do
    assert_raise ArgumentError, ~r/outside the generated range/, fn ->
      SchemaSource.source(:metadata, ApiTable.max_version(:metadata) + 1)
    end
  end

  test "flexible-version metadata agrees with each version's source" do
    spec = SchemaSource.json_spec()

    for api <- ApiTable.apis(), {min, max} = SchemaSource.vsn_range(api), vsn <- min..max do
      ours = vsn >= SchemaSource.min_flexible_vsn(api)

      expected =
        case SchemaSource.source(api, vsn) do
          :kpro -> vsn >= :kpro_schema.min_flexible_vsn(api)
          :json -> Kayrock.Generate.JsonSpec.flexible?(spec, api, vsn)
        end

      assert ours == expected, "#{api} v#{vsn}"
    end
  end

  test "header versions" do
    assert SchemaSource.request_header_vsn(:metadata, 8) == 1
    assert SchemaSource.request_header_vsn(:metadata, 9) == 2
    assert SchemaSource.response_header_vsn(:metadata, 9) == 1
    assert SchemaSource.response_header_vsn(:api_versions, 3) == 0
  end

  test "vendored spec directory and provenance" do
    assert ApiTable.kafka_spec_version() == "4.4.0"
    assert File.dir?(ApiTable.spec_dir())
    provenance = File.read!(Path.join(ApiTable.spec_dir(), "PROVENANCE.md"))
    assert provenance =~ "Tag: `4.4.0`"
    assert File.exists?(Path.join(ApiTable.spec_dir(), "NOTICE"))
  end

  test "vendored specs keep their Apache license headers" do
    for file <- Path.wildcard(Path.join(ApiTable.spec_dir(), "*.json")) do
      assert File.read!(file) =~ ~r/\A\/\/ Licensed to the Apache Software Foundation/,
             "#{file} is missing its license header"
    end
  end
end
