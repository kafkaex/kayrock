defmodule Kayrock.SaslAuthenticateV2Test do
  @moduledoc """
  SaslAuthenticate v2 (first flexible version), generated from the Kafka
  4.4.0 JSON specs. Fixtures are derived by hand from
  `SaslAuthenticateRequest.json` / `SaslAuthenticateResponse.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.SaslAuthenticate.V2

  test "version range" do
    assert Kayrock.SaslAuthenticate.max_vsn() == 2
    assert %V2.Request{} = Kayrock.SaslAuthenticate.get_request_struct(2)
  end

  test "v2 request serializes auth bytes as compact bytes" do
    request = %V2.Request{
      correlation_id: 9,
      client_id: "kayrock",
      auth_bytes: <<0, "alice", 0, "secret">>,
      tagged_fields: []
    }

    expected =
      <<36::16, 2::16, 9::32, 7::16, "kayrock", 0>> <>
        <<14, 0, "alice", 0, "secret">> <> <<0>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v2 response deserializes" do
    binary = <<9::32, 0, 0::16, 0, 1, 3_600_000::64, 0>>

    assert V2.Response.deserialize(binary) ==
             {%V2.Response{
                correlation_id: 9,
                error_code: 0,
                error_message: nil,
                auth_bytes: "",
                session_lifetime_ms: 3_600_000,
                tagged_fields: []
              }, <<>>}
  end

  test "v2 authentication-failure response deserializes" do
    binary = <<9::32, 0, 58::16, 13, "bad password", 1, 0::64, 0>>

    assert {%V2.Response{error_code: 58, error_message: "bad password"}, <<>>} =
             V2.Response.deserialize(binary)
  end
end
