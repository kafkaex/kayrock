defmodule Kayrock.InitProducerIdTest do
  @moduledoc """
  Tests for InitProducerId API (V0-V2).

  API Key: 22
  Used to: Initialize a producer ID for idempotent/transactional producing.

  Protocol structure:
  - V0-V1: Basic init with transactional_id and timeout
  - V2: Flexible/compact format with tagged_fields
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.InitProducerIdFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..2 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = InitProducerIdFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 22
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = InitProducerIdFactory.response_data(version)

        response_module = Module.concat([Kayrock.InitProducerId, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_integer(actual_struct.producer_id)
        assert is_integer(actual_struct.producer_epoch)
      end
    end

    test "all available versions have modules" do
      for version <- 0..2 do
        request_module = Module.concat([Kayrock, InitProducerId, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, InitProducerId, :"V#{version}", Response])

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

  describe "V0 - basic init" do
    alias Kayrock.InitProducerId.V0.Request
    alias Kayrock.InitProducerId.V0.Response

    test "request with transactional_id serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        transactional_id: "my-transaction",
        transaction_timeout_ms: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 22
      assert api_version == 0
    end

    test "request with null transactional_id serializes correctly" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        transactional_id: nil,
        transaction_timeout_ms: 60_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response with valid producer_id deserializes correctly" do
      {response_binary, expected} = InitProducerIdFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.producer_id == 1
      assert response.producer_epoch == 0
      assert response.error_code == 0
    end
  end

  describe "V1 - same schema as V0" do
    alias Kayrock.InitProducerId.V1.Response

    test "response with different producer_id/epoch deserializes correctly" do
      {response_binary, expected} = InitProducerIdFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.producer_id == 42
      assert response.producer_epoch == 5
      assert response.throttle_time_ms == 50
    end
  end

  describe "V2 - flexible/compact format" do
    alias Kayrock.InitProducerId.V2.Request
    alias Kayrock.InitProducerId.V2.Response

    test "request with tagged_fields serializes correctly" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        transactional_id: "txn-v2",
        transaction_timeout_ms: 30_000,
        tagged_fields: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 22
      assert api_version == 2
    end

    test "deserializes response with tagged_fields" do
      {response_binary, expected_struct} = InitProducerIdFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.tagged_fields == []
      assert response.producer_id == 100
      assert response.producer_epoch == 10
      assert response.throttle_time_ms == 100
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, InitProducerId, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: nil,
            transaction_timeout_ms: 60_000
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, InitProducerId, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: nil,
            transaction_timeout_ms: 60_000
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
    for version <- 0..2 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = InitProducerIdFactory.response_data(version)
        response_module = Module.concat([Kayrock.InitProducerId, :"V#{version}", Response])

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
    for version <- 0..2 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = InitProducerIdFactory.response_data(version)
        response_module = Module.concat([Kayrock.InitProducerId, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..2 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.InitProducerId, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 partial response fails with FunctionClauseError" do
      # Only correlation_id and throttle_time_ms, missing error_code etc
      partial = <<0, 0, 0, 0, 0, 0, 0, 0>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.InitProducerId.V0.Response.deserialize(partial)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 53, "TRANSACTIONAL_ID_AUTHORIZATION_FAILED"},
      {1, 51, "CONCURRENT_TRANSACTIONS"},
      {2, 59, "INVALID_PRODUCER_ID_MAPPING"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.InitProducerId, :"V#{version}", Response])

        response_binary = InitProducerIdFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        assert response.error_code == error_code
        assert response.producer_id == -1
        assert response.producer_epoch == -1
      end
    end
  end

  # ============================================
  # Custom Scenarios
  # ============================================

  describe "custom scenarios" do
    test "response with max producer_id deserializes correctly" do
      response_binary =
        InitProducerIdFactory.response_binary(0, producer_id: 9_223_372_036_854_775_807)

      {response, <<>>} = Kayrock.InitProducerId.V0.Response.deserialize(response_binary)

      assert response.producer_id == 9_223_372_036_854_775_807
    end

    test "response with high producer_epoch deserializes correctly" do
      response_binary = InitProducerIdFactory.response_binary(1, producer_epoch: 32767)
      {response, <<>>} = Kayrock.InitProducerId.V1.Response.deserialize(response_binary)

      assert response.producer_epoch == 32767
    end
  end
end
