defmodule Kayrock.Client.ApiVersionsTest do
  use Kayrock.ClientCase

  test "v0", %{client: client} do
    {:ok, resp} = Kayrock.api_versions(client, 0)

    %Kayrock.ApiVersions.V0.Response{error_code: 0, api_versions: versions} = resp

    assert is_list(versions)
    fetch_versions = Enum.find(versions, fn v -> v[:api_key] == 1 end)
    assert fetch_versions[:min_version] == 0
    assert fetch_versions[:max_version] >= 10

    assert Process.alive?(client)
  end

  test "v1", %{client: client} do
    {:ok, resp} = Kayrock.api_versions(client, 1)

    %Kayrock.ApiVersions.V1.Response{error_code: 0, api_versions: versions, throttle_time_ms: 0} =
      resp

    assert is_list(versions)
    fetch_versions = Enum.find(versions, fn v -> v[:api_key] == 1 end)
    assert fetch_versions[:min_version] == 0
    assert fetch_versions[:max_version] >= 10

    assert Process.alive?(client)
  end
end
