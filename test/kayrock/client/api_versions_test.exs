defmodule Kayrock.Client.ApiVersionsTest do
  use ExUnit.Case

  @moduletag :integration

  def test_metadata do
    assert Kayrock.ApiVersions.min_vsn() == 0
    assert Kayrock.ApiVersions.max_vsn() == 1
  end

  test "v0" do
    {:ok, pid} = Kayrock.Client.start_link()

    {:ok, resp} = Kayrock.api_versions(pid, 0)

    %Kayrock.ApiVersions.V0.Response{error_code: 0, api_versions: versions} = resp

    assert is_list(versions)
    fetch_versions = Enum.find(versions, fn v -> v[:api_key] == 1 end)
    assert fetch_versions[:min_version] == 0
    assert fetch_versions[:max_version] == 10

    Kayrock.Client.stop(pid)
  end

  test "v1" do
    {:ok, pid} = Kayrock.Client.start_link()

    {:ok, resp} = Kayrock.api_versions(pid, 1)

    %Kayrock.ApiVersions.V1.Response{error_code: 0, api_versions: versions, throttle_time_ms: 0} =
      resp

    assert is_list(versions)
    fetch_versions = Enum.find(versions, fn v -> v[:api_key] == 1 end)
    assert fetch_versions[:min_version] == 0
    assert fetch_versions[:max_version] == 10

    Kayrock.Client.stop(pid)
  end
end
