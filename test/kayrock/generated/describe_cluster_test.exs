defmodule Kayrock.DescribeClusterTest do
  @moduledoc """
  DescribeCluster (key 60) v0-v2, generated from the Kafka 4.4.0 JSON specs
  (flexible from v0).

  * v1: request and response add `EndpointType` (1 = brokers, 2 = controllers)
  * v2: request adds `IncludeFencedBrokers`; each broker adds `IsFenced`

  Fixtures are derived by hand from `DescribeCluster{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.DescribeCluster

  test "v0-v2 are generated with api key 60" do
    assert {DescribeCluster.min_vsn(), DescribeCluster.max_vsn()} == {0, 2}
    assert DescribeCluster.V0.Request.api_key() == 60
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_cluster) == 60
  end

  describe "requests" do
    test "v0" do
      request = %DescribeCluster.V0.Request{
        correlation_id: 0,
        client_id: "kayrock",
        include_cluster_authorized_operations: true
      }

      assert serialize(request) == request_header(60, 0, 0, "kayrock") <> <<1>> <> tags()
    end

    test "v1 adds endpoint_type" do
      request = %DescribeCluster.V1.Request{
        correlation_id: 1,
        client_id: "kayrock",
        include_cluster_authorized_operations: false,
        endpoint_type: 2
      }

      assert serialize(request) == request_header(60, 1, 1, "kayrock") <> <<0, 2>> <> tags()
    end

    test "v2 adds include_fenced_brokers" do
      request = %DescribeCluster.V2.Request{
        correlation_id: 2,
        client_id: "kayrock",
        include_cluster_authorized_operations: true,
        endpoint_type: 1,
        include_fenced_brokers: true
      }

      assert serialize(request) == request_header(60, 2, 2, "kayrock") <> <<1, 1, 1>> <> tags()
    end
  end

  describe "responses" do
    test "v0" do
      wire =
        response_header(0) <>
          <<0::32, 0::16>> <>
          cstr(nil) <>
          cstr("cluster-1") <>
          <<1::32>> <>
          carray([
            <<1::32>> <> cstr("b1") <> <<9092::32>> <> cstr("rack-a") <> tags(),
            <<2::32>> <> cstr("b2") <> <<9093::32>> <> cstr(nil) <> tags()
          ]) <> <<8096::32>> <> tags()

      assert {response, <<>>} = DescribeCluster.V0.Response.deserialize(wire)

      assert response == %DescribeCluster.V0.Response{
               correlation_id: 0,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               cluster_id: "cluster-1",
               controller_id: 1,
               brokers: [
                 %{broker_id: 1, host: "b1", port: 9092, rack: "rack-a", tagged_fields: []},
                 %{broker_id: 2, host: "b2", port: 9093, rack: nil, tagged_fields: []}
               ],
               cluster_authorized_operations: 8096,
               tagged_fields: []
             }
    end

    test "v1 decodes endpoint_type and an error" do
      wire =
        response_header(1) <>
          <<0::32, 114::16>> <>
          cstr("wrong endpoint type") <>
          <<1>> <>
          cstr("") <> <<-1::32-signed>> <> carray([]) <> <<-2_147_483_648::32-signed>> <> tags()

      assert {response, <<>>} = DescribeCluster.V1.Response.deserialize(wire)

      assert response == %DescribeCluster.V1.Response{
               correlation_id: 1,
               throttle_time_ms: 0,
               error_code: 114,
               error_message: "wrong endpoint type",
               endpoint_type: 1,
               cluster_id: "",
               controller_id: -1,
               brokers: [],
               cluster_authorized_operations: -2_147_483_648,
               tagged_fields: []
             }
    end

    test "v2 decodes is_fenced" do
      wire =
        response_header(2) <>
          <<0::32, 0::16>> <>
          cstr(nil) <>
          <<1>> <>
          cstr("cluster-1") <>
          <<1::32>> <>
          carray([<<1::32>> <> cstr("b1") <> <<9092::32>> <> cstr(nil) <> <<1>> <> tags()]) <>
          <<0::32>> <> tags()

      assert {response, <<>>} = DescribeCluster.V2.Response.deserialize(wire)

      assert response.brokers == [
               %{broker_id: 1, host: "b1", port: 9092, rack: nil, is_fenced: 1, tagged_fields: []}
             ]

      assert response.endpoint_type == 1
      assert DescribeCluster.deserialize(2, wire) == {response, <<>>}
    end
  end
end
