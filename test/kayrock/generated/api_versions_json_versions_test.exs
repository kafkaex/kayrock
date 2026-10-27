defmodule Kayrock.ApiVersionsJsonVersionsTest do
  @moduledoc """
  ApiVersions (key 18) v4-v5, generated from the Kafka 4.4.0 JSON specs.

  * v4: same schema as v3 (KAFKA-17011: SupportedFeatures.MinVersion may be 0)
  * v5: request adds nullable `ClusterId` and `NodeId` (KIP-1242)

  The response always uses response header v0 (KIP-511) and carries the
  named tagged fields SupportedFeatures (0), FinalizedFeaturesEpoch (1),
  FinalizedFeatures (2) and ZkMigrationReady (3).

  Fixtures are derived by hand from `ApiVersions{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.ApiVersions

  test "v0-v5 are generated" do
    assert ApiVersions.min_vsn() == 0
    assert ApiVersions.max_vsn() == 5
    assert %ApiVersions.V4.Request{} = ApiVersions.get_request_struct(4)
    assert %ApiVersions.V5.Request{} = ApiVersions.get_request_struct(5)
  end

  describe "requests" do
    test "v4 serializes like v3 with the v4 version" do
      request = %ApiVersions.V4.Request{
        correlation_id: 4,
        client_id: "kayrock",
        client_software_name: "kayrock",
        client_software_version: "1.0.0"
      }

      assert serialize(request) ==
               request_header(18, 4, 4, "kayrock") <>
                 cstr("kayrock") <> cstr("1.0.0") <> tags()
    end

    test "v5 serializes cluster_id and node_id" do
      request = %ApiVersions.V5.Request{
        correlation_id: 5,
        client_id: "kayrock",
        client_software_name: "kayrock",
        client_software_version: "1.0.0",
        cluster_id: "4L6g3nShT-eMCtK--X86sw",
        node_id: 1
      }

      assert serialize(request) ==
               request_header(18, 5, 5, "kayrock") <>
                 cstr("kayrock") <>
                 cstr("1.0.0") <> cstr("4L6g3nShT-eMCtK--X86sw") <> <<1::32>> <> tags()
    end

    test "v5 serializes a null cluster_id and node_id -1" do
      request = %ApiVersions.V5.Request{
        correlation_id: 5,
        client_id: nil,
        client_software_name: "k",
        client_software_version: "1",
        cluster_id: nil,
        node_id: -1
      }

      assert serialize(request) ==
               request_header(18, 5, 5, nil) <>
                 cstr("k") <> cstr("1") <> <<0>> <> <<-1::32-signed>> <> tags()
    end
  end

  describe "responses" do
    for version <- [4, 5] do
      test "v#{version} decodes api keys and every named tagged field" do
        version = unquote(version)
        module = Module.concat([ApiVersions, :"V#{version}", Response])

        supported =
          carray([{"metadata.version", 1, 29}, {"kraft.version", 0, 1}], fn {n, lo, hi} ->
            cstr(n) <> <<lo::16, hi::16>> <> tags()
          end)

        finalized =
          carray([{"metadata.version", 29, 29}], fn {n, max, min} ->
            cstr(n) <> <<max::16, min::16>> <> tags()
          end)

        tag_buffer =
          uvarint(4) <>
            tag(0, supported) <>
            tag(1, <<17::64>>) <> tag(2, finalized) <> tag(3, <<1>>)

        wire =
          <<version::32, 0::16>> <>
            carray([{0, 0, 13}, {18, 0, 5}], fn {k, lo, hi} ->
              <<k::16, lo::16, hi::16>> <> tags()
            end) <> <<0::32>> <> tag_buffer

        assert {response, <<>>} = module.deserialize(wire)

        assert response == %{
                 __struct__: module,
                 correlation_id: version,
                 error_code: 0,
                 api_keys: [
                   %{api_key: 0, min_version: 0, max_version: 13, tagged_fields: []},
                   %{api_key: 18, min_version: 0, max_version: 5, tagged_fields: []}
                 ],
                 throttle_time_ms: 0,
                 supported_features: [
                   %{
                     name: "metadata.version",
                     min_version: 1,
                     max_version: 29,
                     tagged_fields: []
                   },
                   %{name: "kraft.version", min_version: 0, max_version: 1, tagged_fields: []}
                 ],
                 finalized_features_epoch: 17,
                 finalized_features: [
                   %{
                     name: "metadata.version",
                     max_version_level: 29,
                     min_version_level: 29,
                     tagged_fields: []
                   }
                 ],
                 zk_migration_ready: 1,
                 tagged_fields: []
               }

        assert ApiVersions.deserialize(version, wire) == {response, <<>>}
      end

      test "v#{version} absent tagged fields take their defaults" do
        version = unquote(version)
        module = Module.concat([ApiVersions, :"V#{version}", Response])

        # error 129 (REBOOTSTRAP_REQUIRED) as returned for a mismatched cluster id
        wire = <<version::32, 129::16>> <> carray([]) <> <<0::32>> <> tags()

        assert {response, <<>>} = module.deserialize(wire)
        assert response.error_code == 129
        assert response.api_keys == []
        assert response.supported_features == []
        assert response.finalized_features_epoch == -1
        assert response.finalized_features == []
        assert response.zk_migration_ready == 0
      end
    end
  end
end
