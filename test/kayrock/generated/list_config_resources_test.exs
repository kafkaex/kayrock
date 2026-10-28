defmodule Kayrock.ListConfigResourcesTest do
  @moduledoc """
  ListConfigResources (key 74) v0-v1, generated from the Kafka 4.4.0 JSON
  specs (flexible from v0). Formerly ListClientMetricsResources (KIP-1142).

  * v0: empty request; the response lists client-metrics resource names
  * v1: the request adds `ResourceTypes` (empty = default types); each
    resource adds `ResourceType`

  Fixtures are derived by hand from `ListConfigResources{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.ListConfigResources, as: LCR

  test "v0-v1 are generated with api key 74" do
    assert {LCR.min_vsn(), LCR.max_vsn()} == {0, 1}
    assert LCR.V0.Request.api_key() == 74
  end

  test "v0 request is just the header and an empty tag buffer" do
    request = %LCR.V0.Request{correlation_id: 0, client_id: "kayrock"}
    assert serialize(request) == request_header(74, 0, 0, "kayrock") <> tags()
  end

  test "v1 request serializes resource types" do
    request = %LCR.V1.Request{correlation_id: 1, client_id: "kayrock", resource_types: [2, 4, 16]}

    assert serialize(request) ==
             request_header(74, 1, 1, "kayrock") <> carray([<<2>>, <<4>>, <<16>>]) <> tags()
  end

  test "v0 response decodes resource names" do
    wire =
      response_header(0) <>
        <<0::32, 0::16>> <>
        carray([cstr("metrics-1") <> tags(), cstr("metrics-2") <> tags()]) <> tags()

    assert {response, <<>>} = LCR.V0.Response.deserialize(wire)

    assert response == %LCR.V0.Response{
             correlation_id: 0,
             throttle_time_ms: 0,
             error_code: 0,
             config_resources: [
               %{resource_name: "metrics-1", tagged_fields: []},
               %{resource_name: "metrics-2", tagged_fields: []}
             ],
             tagged_fields: []
           }
  end

  test "v1 response decodes resource types" do
    wire =
      response_header(1) <>
        <<0::32, 0::16>> <>
        carray([cstr("1") <> <<4>> <> tags(), cstr("orders") <> <<2>> <> tags()]) <> tags()

    assert {response, <<>>} = LCR.V1.Response.deserialize(wire)

    assert response.config_resources == [
             %{resource_name: "1", resource_type: 4, tagged_fields: []},
             %{resource_name: "orders", resource_type: 2, tagged_fields: []}
           ]

    assert LCR.deserialize(1, wire) == {response, <<>>}
  end
end
