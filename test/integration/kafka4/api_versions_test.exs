defmodule Kayrock.Kafka4.ApiVersionsJsonVersionsTest do
  @moduledoc """
  ApiVersions v4-v5 against apache/kafka:4.4.0: the broker accepts both
  versions and the responses (including the named tagged fields) decode with
  no leftover bytes. v5 checks the ClusterId / NodeId the client sends
  (KIP-1242): a mismatch is answered with REBOOTSTRAP_REQUIRED (129).
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @rebootstrap_required 129

  test "v4 returns api keys and features", %{kafka: kafka} do
    request = %Kayrock.ApiVersions.V4.Request{
      client_software_name: "kayrock",
      client_software_version: "kafka4"
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == 0

    versions = Map.new(response.api_keys, &{&1.api_key, {&1.min_version, &1.max_version}})
    assert versions[18] == {0, 5}
    assert versions[60] == {0, 2}

    assert Enum.any?(response.supported_features, &(&1.name == "metadata.version"))
    assert Enum.any?(response.finalized_features, &(&1.name == "metadata.version"))
    assert response.finalized_features_epoch >= 0
  end

  test "v5 with the right cluster and node id succeeds", %{kafka: kafka} do
    request = %Kayrock.ApiVersions.V5.Request{
      client_software_name: "kayrock",
      client_software_version: "kafka4",
      cluster_id: cluster_id(),
      node_id: node_id()
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == 0
    assert length(response.api_keys) > 50
  end

  test "v5 without a cluster id succeeds", %{kafka: kafka} do
    request = %Kayrock.ApiVersions.V5.Request{
      client_software_name: "kayrock",
      client_software_version: "kafka4",
      cluster_id: nil,
      node_id: -1
    }

    assert {%{error_code: 0}, <<>>} = raw_call(kafka, request)
  end

  test "v5 with another cluster id gets REBOOTSTRAP_REQUIRED", %{kafka: kafka} do
    request = %Kayrock.ApiVersions.V5.Request{
      client_software_name: "kayrock",
      client_software_version: "kafka4",
      cluster_id: "some-other-cluster-id0",
      node_id: node_id()
    }

    assert {response, <<>>} = raw_call(kafka, request)
    assert response.error_code == @rebootstrap_required
    assert response.api_keys == []
  end
end
