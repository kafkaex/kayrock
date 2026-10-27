defmodule Kayrock.FindCoordinatorBatchedTest do
  @moduledoc """
  FindCoordinator v4-v6 (JSON-sourced; v0-v3 come from `:kpro_schema` and are
  covered by `Kayrock.FindCoordinatorTest`).

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `FindCoordinatorRequest.json` / `FindCoordinatorResponse.json` specs. v4
  batches lookups (KIP-699: `coordinator_keys[]` in, `coordinators[]` out);
  v5 (TRANSACTION_ABORTABLE) and v6 (share-group key type 2, KIP-932) keep
  the v4 layout.
  """
  use ExUnit.Case, async: true

  alias Kayrock.FindCoordinator

  defp request_header(vsn), do: <<10::16, vsn::16, 17::32, 7::16, "kayrock", 0>>

  test "version range and dispatch" do
    assert FindCoordinator.max_vsn() == 6

    for vsn <- 4..6 do
      assert Kayrock.Request.api_vsn(FindCoordinator.get_request_struct(vsn)) == vsn
    end
  end

  for vsn <- 4..6 do
    test "V#{vsn} batched request" do
      vsn = unquote(vsn)

      request = %{
        FindCoordinator.get_request_struct(vsn)
        | correlation_id: 17,
          client_id: "kayrock",
          key_type: 0,
          coordinator_keys: ["g1", "g2"]
      }

      refute Map.has_key?(request, :key)

      # key_type 0 (group), coordinator_keys ["g1", "g2"], tag buffer
      expected = request_header(vsn) <> <<0, 3, 3, "g1", 3, "g2", 0>>
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "V#{vsn} batched response" do
      vsn = unquote(vsn)

      data =
        IO.iodata_to_binary([
          <<17::32, 0>>,
          # throttle_time_ms; coordinators: 2 elements
          <<0::32, 3>>,
          # {"g1", node 1, "localhost", 9092, error 0, message null}
          <<3, "g1", 1::32, 10, "localhost", 9092::32, 0::16, 0, 0>>,
          # {"g2", node -1, "", -1, COORDINATOR_NOT_AVAILABLE (15), "busy"}
          <<3, "g2", -1::32-signed, 1, -1::32-signed, 15::16, 5, "busy", 0>>,
          <<0>>
        ])

      assert {response, <<>>} = FindCoordinator.deserialize(vsn, data)
      assert response.correlation_id == 17
      assert response.throttle_time_ms == 0

      assert response.coordinators == [
               %{
                 key: "g1",
                 node_id: 1,
                 host: "localhost",
                 port: 9092,
                 error_code: 0,
                 error_message: nil,
                 tagged_fields: []
               },
               %{
                 key: "g2",
                 node_id: -1,
                 host: "",
                 port: -1,
                 error_code: 15,
                 error_message: "busy",
                 tagged_fields: []
               }
             ]
    end
  end
end
