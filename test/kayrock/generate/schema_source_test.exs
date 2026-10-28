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

  test "this table generates exactly what kpro_schema defines (no new versions yet)" do
    for api <- ApiTable.apis() do
      assert SchemaSource.vsn_range(api) == :kpro_schema.vsn_range(api), "#{api}"
    end
  end

  test "versions outside the table raise" do
    assert_raise ArgumentError, ~r/outside the generated range/, fn ->
      SchemaSource.source(:metadata, ApiTable.max_version(:metadata) + 1)
    end
  end

  test "flexible-version metadata agrees with :kpro_schema for generated versions" do
    for api <- ApiTable.apis(), {min, max} = SchemaSource.vsn_range(api), vsn <- min..max do
      ours = vsn >= SchemaSource.min_flexible_vsn(api)
      kpro = vsn >= :kpro_schema.min_flexible_vsn(api)
      assert ours == kpro, "#{api} v#{vsn}"
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
