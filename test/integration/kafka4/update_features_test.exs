defmodule Kayrock.Kafka4.UpdateFeaturesTest do
  @moduledoc """
  UpdateFeatures v0-v2 against apache/kafka:4.4.0. Each version re-applies
  the finalized `transaction.version` level (a no-op) and v1-v2 also
  validate an upgrade without applying it; an unknown feature is rejected.
  Every response must decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @invalid_update_version 95
  @upgrade 1

  defp finalized_level(kafka, feature) do
    {%{finalized_features: finalized}, <<>>} =
      raw_call(kafka, %Kayrock.ApiVersions.V4.Request{
        client_software_name: "kayrock",
        client_software_version: "kafka4"
      })

    Enum.find_value(finalized, fn %{name: name, max_version_level: level} ->
      if name == feature, do: level
    end)
  end

  test "v0 re-applies the finalized level", %{kafka: kafka} do
    level = finalized_level(kafka, "transaction.version")

    request = %Kayrock.UpdateFeatures.V0.Request{
      timeout_ms: 10_000,
      feature_updates: [
        %{feature: "transaction.version", max_version_level: level, allow_downgrade: false}
      ]
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == 0
    assert [%{feature: "transaction.version", error_code: 0}] = response.results
  end

  for version <- [1, 2] do
    test "v#{version} validate_only and an unknown feature", %{kafka: kafka} do
      version = unquote(version)
      module = Module.concat([Kayrock.UpdateFeatures, :"V#{version}", Request])
      level = finalized_level(kafka, "transaction.version")

      request =
        struct(module,
          timeout_ms: 10_000,
          feature_updates: [
            %{feature: "transaction.version", max_version_level: level, upgrade_type: @upgrade}
          ],
          validate_only: true
        )

      assert {response, <<>>} = raw_call(kafka, request)
      assert response.error_code == 0
      if version == 1, do: assert([%{error_code: 0}] = response.results)

      unknown =
        struct(module,
          timeout_ms: 10_000,
          feature_updates: [
            %{feature: "kayrock.bogus", max_version_level: 1, upgrade_type: @upgrade}
          ],
          validate_only: true
        )

      assert {response, <<>>} = raw_call(kafka, unknown)
      assert response.error_code == @invalid_update_version
      assert response.error_message =~ "kayrock.bogus"
    end
  end
end
