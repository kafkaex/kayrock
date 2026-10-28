defmodule Kayrock.UnregisterBrokerTest do
  @moduledoc """
  UnregisterBroker (key 64) v0, generated from the Kafka 4.4.0 JSON specs
  (flexible). Used by admin clients to remove a KRaft broker registration
  (KIP-500 / KIP-631); brokers forward it to the controller.

  Fixtures are derived by hand from `UnregisterBroker{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.UnregisterBroker

  test "v0 is generated with api key 64" do
    assert {UnregisterBroker.min_vsn(), UnregisterBroker.max_vsn()} == {0, 0}
    assert UnregisterBroker.V0.Request.api_key() == 64
  end

  test "v0 request" do
    request = %UnregisterBroker.V0.Request{
      correlation_id: 3,
      client_id: "kayrock",
      broker_id: 999
    }

    assert serialize(request) == request_header(64, 0, 3, "kayrock") <> <<999::32>> <> tags()
  end

  test "v0 response decodes an error" do
    wire =
      response_header(3) <>
        <<0::32, 102::16>> <> cstr("Broker ID 999 is not currently registered") <> tags()

    assert {response, <<>>} = UnregisterBroker.V0.Response.deserialize(wire)

    assert response == %UnregisterBroker.V0.Response{
             correlation_id: 3,
             throttle_time_ms: 0,
             error_code: 102,
             error_message: "Broker ID 999 is not currently registered",
             tagged_fields: []
           }

    assert UnregisterBroker.deserialize(0, wire) == {response, <<>>}
  end

  test "v0 response decodes success" do
    wire = response_header(4) <> <<10::32, 0::16>> <> cstr(nil) <> tags()

    assert {%UnregisterBroker.V0.Response{
              throttle_time_ms: 10,
              error_code: 0,
              error_message: nil
            },
            <<>>} =
             UnregisterBroker.V0.Response.deserialize(wire)
  end
end
