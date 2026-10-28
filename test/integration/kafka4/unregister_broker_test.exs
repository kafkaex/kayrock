defmodule Kayrock.Kafka4.UnregisterBrokerTest do
  @moduledoc """
  UnregisterBroker v0 against apache/kafka:4.4.0. Unregistering the only
  (live) broker would break the shared container, so the test unregisters
  an id that was never registered and expects BROKER_ID_NOT_REGISTERED
  (102). The response must decode with no leftover bytes.
  """
  use Kayrock.Kafka4Case

  import Kayrock.Kafka4AdminHelpers

  @broker_id_not_registered 102

  test "v0 with an unknown broker id", %{kafka: kafka} do
    assert {response, <<>>} =
             raw_call(kafka, %Kayrock.UnregisterBroker.V0.Request{broker_id: 999})

    assert response.error_code == @broker_id_not_registered
    assert response.error_message =~ "999"
  end
end
