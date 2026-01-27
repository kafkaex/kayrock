defmodule Kayrock.SaslAuthenticateTest do
  @moduledoc """
  Tests for SaslAuthenticate API (V0-V1).

  API Key: 36
  Used to: Perform SASL authentication after handshake.

  Protocol structure:
  - V0: Basic auth with bytes exchange
  - V1: Adds session_lifetime_ms in response
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.SaslAuthenticateFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = SaslAuthenticateFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 36
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = SaslAuthenticateFactory.response_data(version)

        response_module = Module.concat([Kayrock.SaslAuthenticate, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert actual_struct.error_code == 0
        assert is_binary(actual_struct.auth_bytes)
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, SaslAuthenticate, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, SaslAuthenticate, :"V#{version}", Response])

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

  describe "V0 - basic SASL auth" do
    alias Kayrock.SaslAuthenticate.V0.Request
    alias Kayrock.SaslAuthenticate.V0.Response

    test "request with auth_bytes serializes correctly" do
      auth_bytes = <<0, "testuser", 0, "testpassword">>

      request = %Request{
        correlation_id: 1,
        client_id: "test",
        auth_bytes: auth_bytes
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 36
      assert api_version == 0
    end

    test "response deserializes auth_bytes correctly" do
      {response_binary, expected} = SaslAuthenticateFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.auth_bytes == "response"
      assert response.error_code == 0
      assert response.error_message == nil
    end
  end

  describe "V1 - adds session_lifetime_ms" do
    alias Kayrock.SaslAuthenticate.V1.Response

    test "response with session_lifetime_ms deserializes correctly" do
      {response_binary, expected} = SaslAuthenticateFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.session_lifetime_ms == 3_600_000
      assert response.auth_bytes == "response"
      assert response.error_code == 0
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, SaslAuthenticate, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            auth_bytes: <<>>
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, SaslAuthenticate, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            auth_bytes: <<>>
          )

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
        {response_binary, _} = SaslAuthenticateFactory.response_data(version)
        response_module = Module.concat([Kayrock.SaslAuthenticate, :"V#{version}", Response])

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
        {response_binary, _} = SaslAuthenticateFactory.response_data(version)
        response_module = Module.concat([Kayrock.SaslAuthenticate, :"V#{version}", Response])

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
        response_module = Module.concat([Kayrock.SaslAuthenticate, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 partial response fails with FunctionClauseError" do
      # Only correlation_id and error_code
      partial = <<0, 0, 0, 0, 0, 0>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.SaslAuthenticate.V0.Response.deserialize(partial)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 34, "SASL_AUTHENTICATION_FAILED"},
      {1, 58, "ILLEGAL_SASL_STATE"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.SaslAuthenticate, :"V#{version}", Response])

        response_binary =
          SaslAuthenticateFactory.error_response(version, error_code: error_code)

        {response, <<>>} = response_module.deserialize(response_binary)

        assert response.error_code == error_code
        assert response.error_message == "Authentication failed"
      end
    end
  end
end
