defmodule Kayrock.DelegationTokensFlexibleTest do
  @moduledoc """
  Create/Renew/Expire/DescribeDelegationToken versions generated from the
  Kafka 4.4.0 JSON specs: CreateDelegationToken v3 (KIP-373 owner and
  requester principals), Renew/Expire v2 and DescribeDelegationToken v2-v3
  (v2 is the first flexible version, v3 adds the token requester).

  Fixtures are derived by hand from the JSON specs.
  """
  use ExUnit.Case, async: true

  @hmac <<1, 2, 3, 4>>

  defp header(api_key, vsn, correlation_id),
    do: <<api_key::16, vsn::16, correlation_id::32, 7::16, "kayrock", 0>>

  test "version ranges" do
    assert Kayrock.CreateDelegationToken.max_vsn() == 3
    assert Kayrock.RenewDelegationToken.max_vsn() == 2
    assert Kayrock.ExpireDelegationToken.max_vsn() == 2
    assert Kayrock.DescribeDelegationToken.max_vsn() == 3
  end

  describe "CreateDelegationToken v3" do
    alias Kayrock.CreateDelegationToken.V3

    test "request serializes" do
      request = %V3.Request{
        correlation_id: 1,
        client_id: "kayrock",
        owner_principal_type: "User",
        owner_principal_name: "bob",
        renewers: [%{principal_type: "User", principal_name: "carol", tagged_fields: []}],
        max_lifetime_ms: -1,
        tagged_fields: []
      }

      expected =
        header(38, 3, 1) <>
          <<5, "User", 4, "bob">> <>
          <<2, 5, "User", 6, "carol", 0>> <>
          <<-1::64-signed, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "request with null owner serializes" do
      request = %V3.Request{
        correlation_id: 1,
        client_id: "kayrock",
        owner_principal_type: nil,
        owner_principal_name: nil,
        renewers: [],
        max_lifetime_ms: 1000,
        tagged_fields: []
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               header(38, 3, 1) <> <<0, 0, 1, 1000::64, 0>>
    end

    test "response deserializes" do
      binary =
        <<1::32, 0>> <>
          <<0::16, 5, "User", 4, "bob", 5, "User", 6, "alice">> <>
          <<10::64, 20::64, 30::64, 4, "tok", 5, 1, 2, 3, 4, 0::32, 0>>

      assert V3.Response.deserialize(binary) ==
               {%V3.Response{
                  correlation_id: 1,
                  error_code: 0,
                  principal_type: "User",
                  principal_name: "bob",
                  token_requester_principal_type: "User",
                  token_requester_principal_name: "alice",
                  issue_timestamp_ms: 10,
                  expiry_timestamp_ms: 20,
                  max_timestamp_ms: 30,
                  token_id: "tok",
                  hmac: @hmac,
                  throttle_time_ms: 0,
                  tagged_fields: []
                }, <<>>}
    end
  end

  describe "RenewDelegationToken v2 / ExpireDelegationToken v2" do
    test "renew request serializes" do
      request = %Kayrock.RenewDelegationToken.V2.Request{
        correlation_id: 2,
        client_id: "kayrock",
        hmac: @hmac,
        renew_period_ms: 5000,
        tagged_fields: []
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               header(39, 2, 2) <> <<5, 1, 2, 3, 4, 5000::64, 0>>
    end

    test "expire request serializes" do
      request = %Kayrock.ExpireDelegationToken.V2.Request{
        correlation_id: 3,
        client_id: "kayrock",
        hmac: @hmac,
        expiry_time_period_ms: -1,
        tagged_fields: []
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               header(40, 2, 3) <> <<5, 1, 2, 3, 4, -1::64-signed, 0>>
    end

    test "responses deserialize" do
      binary = <<2::32, 0, 65::16, 99::64, 1::32, 0>>

      for mod <- [Kayrock.RenewDelegationToken.V2, Kayrock.ExpireDelegationToken.V2] do
        response = Module.concat(mod, Response)

        assert response.deserialize(binary) ==
                 {struct(response,
                    correlation_id: 2,
                    error_code: 65,
                    expiry_timestamp_ms: 99,
                    throttle_time_ms: 1,
                    tagged_fields: []
                  ), <<>>}
      end
    end
  end

  describe "DescribeDelegationToken v2-v3" do
    test "request serializes (owners list and null owners)" do
      for vsn <- 2..3 do
        request_mod = Module.concat([Kayrock.DescribeDelegationToken, "V#{vsn}", Request])

        request =
          struct(request_mod,
            correlation_id: vsn,
            client_id: "kayrock",
            owners: [%{principal_type: "User", principal_name: "bob", tagged_fields: []}],
            tagged_fields: []
          )

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
                 header(41, vsn, vsn) <> <<2, 5, "User", 4, "bob", 0, 0>>

        # null owners = describe all tokens
        request = %{request | owners: nil}

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
                 header(41, vsn, vsn) <> <<0, 0>>
      end
    end

    test "v2 response deserializes" do
      binary =
        <<2::32, 0, 0::16>> <>
          <<2>> <>
          <<5, "User", 4, "bob", 10::64, 20::64, 30::64, 4, "tok", 5, 1, 2, 3, 4>> <>
          <<2, 5, "User", 6, "carol", 0>> <>
          <<0>> <>
          <<0::32, 0>>

      assert Kayrock.DescribeDelegationToken.V2.Response.deserialize(binary) ==
               {%Kayrock.DescribeDelegationToken.V2.Response{
                  correlation_id: 2,
                  error_code: 0,
                  tokens: [
                    %{
                      principal_type: "User",
                      principal_name: "bob",
                      issue_timestamp: 10,
                      expiry_timestamp: 20,
                      max_timestamp: 30,
                      token_id: "tok",
                      hmac: @hmac,
                      renewers: [
                        %{principal_type: "User", principal_name: "carol", tagged_fields: []}
                      ],
                      tagged_fields: []
                    }
                  ],
                  throttle_time_ms: 0,
                  tagged_fields: []
                }, <<>>}
    end

    test "v3 response deserializes with token requester" do
      binary =
        <<3::32, 0, 0::16>> <>
          <<2>> <>
          <<5, "User", 4, "bob", 5, "User", 6, "alice">> <>
          <<10::64, 20::64, 30::64, 4, "tok", 5, 1, 2, 3, 4>> <>
          <<1>> <>
          <<0>> <>
          <<0::32, 0>>

      assert Kayrock.DescribeDelegationToken.V3.Response.deserialize(binary) ==
               {%Kayrock.DescribeDelegationToken.V3.Response{
                  correlation_id: 3,
                  error_code: 0,
                  tokens: [
                    %{
                      principal_type: "User",
                      principal_name: "bob",
                      token_requester_principal_type: "User",
                      token_requester_principal_name: "alice",
                      issue_timestamp: 10,
                      expiry_timestamp: 20,
                      max_timestamp: 30,
                      token_id: "tok",
                      hmac: @hmac,
                      renewers: [],
                      tagged_fields: []
                    }
                  ],
                  throttle_time_ms: 0,
                  tagged_fields: []
                }, <<>>}
    end
  end
end
