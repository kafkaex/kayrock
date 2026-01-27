defmodule Kayrock.SaslHandshakeTest do
  @moduledoc """
  Tests for SaslHandshake API (V0-V1).

  API Key: 17
  Used to: Initiate SASL authentication handshake with the broker.

  Protocol structure:
  - V0-V1: Request mechanism, response with available mechanisms
  - V0 vs V1: Minor version bump for compatibility
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.SaslHandshakeFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = SaslHandshakeFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 17
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = SaslHandshakeFactory.response_data(version)

        response_module = Module.concat([Kayrock.SaslHandshake, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.mechanisms)
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, SaslHandshake, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, SaslHandshake, :"V#{version}", Response])

        assert Code.ensure_loaded?(request_module),
               "Request module #{inspect(request_module)} should exist"

        assert Code.ensure_loaded?(response_module),
               "Response module #{inspect(response_module)} should exist"
      end
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic handshake" do
    alias Kayrock.SaslHandshake.V0.Request
    alias Kayrock.SaslHandshake.V0.Response

    test "request with PLAIN mechanism serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        mechanism: "PLAIN"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 17
      assert api_version == 0
    end

    test "response with multiple mechanisms deserializes correctly" do
      {response_binary, expected} = SaslHandshakeFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert "PLAIN" in response.mechanisms
      assert "SCRAM-SHA-256" in response.mechanisms
    end
  end

  describe "V1 - compatible handshake" do
    alias Kayrock.SaslHandshake.V1.Request
    alias Kayrock.SaslHandshake.V1.Response

    test "request with SCRAM mechanism serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        mechanism: "SCRAM-SHA-512"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 17
      assert api_version == 1
    end

    test "response with three mechanisms deserializes correctly" do
      {response_binary, expected} = SaslHandshakeFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert length(response.mechanisms) == 3
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, SaslHandshake, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test", mechanism: "PLAIN")

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, SaslHandshake, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test", mechanism: "PLAIN")

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases - Truncated Binary
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..1 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = SaslHandshakeFactory.response_data(version)
        response_module = Module.concat([Kayrock.SaslHandshake, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  # ============================================
  # Edge Cases - Extra Bytes
  # ============================================

  describe "extra bytes handling" do
    for version <- 0..1 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = SaslHandshakeFactory.response_data(version)
        response_module = Module.concat([Kayrock.SaslHandshake, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..1 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.SaslHandshake, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 invalid mechanisms array length fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # Claims 500 mechanisms (but no data follows)
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.SaslHandshake.V0.Response.deserialize(invalid)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 33, "UNSUPPORTED_SASL_MECHANISM"},
      {1, 34, "ILLEGAL_SASL_STATE"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.SaslHandshake, :"V#{version}", Response])

        response_binary = SaslHandshakeFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        assert response.error_code == error_code
        assert response.mechanisms == []
      end
    end
  end

  # ============================================
  # Custom Scenarios
  # ============================================

  describe "custom scenarios" do
    test "empty mechanisms array deserializes correctly" do
      response_binary = SaslHandshakeFactory.error_response(0, error_code: 33)
      {response, <<>>} = Kayrock.SaslHandshake.V0.Response.deserialize(response_binary)

      assert response.mechanisms == []
    end

    test "single mechanism deserializes correctly" do
      response_binary = SaslHandshakeFactory.response_with_mechanisms(0, ["PLAIN"])
      {response, <<>>} = Kayrock.SaslHandshake.V0.Response.deserialize(response_binary)

      assert response.mechanisms == ["PLAIN"]
    end
  end
end
