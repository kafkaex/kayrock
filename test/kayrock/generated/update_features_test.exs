defmodule Kayrock.UpdateFeaturesTest do
  @moduledoc """
  UpdateFeatures (key 57) v0-v2, generated from the Kafka 4.4.0 JSON specs
  (flexible from v0).

  * v0: per-update `AllowDowngrade`
  * v1: `AllowDowngrade` is replaced by `UpgradeType` (int8); the request adds
    `ValidateOnly`
  * v2: the response drops the per-feature `Results`

  Fixtures are derived by hand from `UpdateFeatures{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.UpdateFeatures

  test "v0-v2 are generated with api key 57" do
    assert {UpdateFeatures.min_vsn(), UpdateFeatures.max_vsn()} == {0, 2}
    assert UpdateFeatures.V0.Request.api_key() == 57
  end

  test "v0 request serializes allow_downgrade" do
    request = %UpdateFeatures.V0.Request{
      correlation_id: 0,
      client_id: "kayrock",
      timeout_ms: 60_000,
      feature_updates: [
        %{feature: "transaction.version", max_version_level: 2, allow_downgrade: true}
      ]
    }

    assert serialize(request) ==
             request_header(57, 0, 0, "kayrock") <>
               <<60_000::32>> <>
               carray([cstr("transaction.version") <> <<2::16, 1>> <> tags()]) <> tags()
  end

  for version <- [1, 2] do
    test "v#{version} request serializes upgrade_type and validate_only" do
      version = unquote(version)

      request =
        struct(Module.concat([UpdateFeatures, :"V#{version}", Request]),
          correlation_id: version,
          client_id: "kayrock",
          timeout_ms: 5_000,
          feature_updates: [
            %{feature: "transaction.version", max_version_level: 2, upgrade_type: 1},
            %{feature: "group.version", max_version_level: 0, upgrade_type: 3}
          ],
          validate_only: true
        )

      assert serialize(request) ==
               request_header(57, version, version, "kayrock") <>
                 <<5_000::32>> <>
                 carray([
                   cstr("transaction.version") <> <<2::16, 1>> <> tags(),
                   cstr("group.version") <> <<0::16, 3>> <> tags()
                 ]) <> <<1>> <> tags()
    end
  end

  for version <- [0, 1] do
    test "v#{version} response decodes per-feature results" do
      version = unquote(version)
      module = Module.concat([UpdateFeatures, :"V#{version}", Response])

      wire =
        response_header(version) <>
          <<0::32, 0::16>> <>
          cstr(nil) <>
          carray([
            cstr("transaction.version") <> <<0::16>> <> cstr(nil) <> tags(),
            cstr("bogus") <> <<42::16>> <> cstr("unknown feature") <> tags()
          ]) <> tags()

      assert {response, <<>>} = module.deserialize(wire)

      assert response == %{
               __struct__: module,
               correlation_id: version,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               results: [
                 %{
                   feature: "transaction.version",
                   error_code: 0,
                   error_message: nil,
                   tagged_fields: []
                 },
                 %{
                   feature: "bogus",
                   error_code: 42,
                   error_message: "unknown feature",
                   tagged_fields: []
                 }
               ],
               tagged_fields: []
             }
    end
  end

  test "v2 response has no results" do
    wire = response_header(2) <> <<0::32, 42::16>> <> cstr("bad update") <> tags()

    assert {response, <<>>} = UpdateFeatures.V2.Response.deserialize(wire)

    assert response == %UpdateFeatures.V2.Response{
             correlation_id: 2,
             throttle_time_ms: 0,
             error_code: 42,
             error_message: "bad update",
             tagged_fields: []
           }

    assert UpdateFeatures.deserialize(2, wire) == {response, <<>>}
  end
end
