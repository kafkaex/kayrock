defmodule Kayrock.AddOffsetsToTxnTest do
  @moduledoc """
  Tests for AddOffsetsToTxn API (V0-V1).

  API Key: 25
  Used to: Add offsets to a transaction for a consumer group.

  Protocol structure:
  - V0-V1: Same schema - throttle_time_ms and error_code in response
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.AddOffsetsToTxnFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = AddOffsetsToTxnFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 25
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = AddOffsetsToTxnFactory.response_data(version)

        response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert actual_struct.error_code == 0
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, AddOffsetsToTxn, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, AddOffsetsToTxn, :"V#{version}", Response])

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

  describe "V0 - basic add offsets" do
    alias Kayrock.AddOffsetsToTxn.V0.Request
    alias Kayrock.AddOffsetsToTxn.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        transactional_id: "my-txn",
        producer_id: 100,
        producer_epoch: 5,
        group_id: "my-group"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 25
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = AddOffsetsToTxnFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0
      assert response.error_code == 0
    end
  end

  describe "V1 - same schema as V0" do
    alias Kayrock.AddOffsetsToTxn.V1.Response

    test "response with throttle_time_ms deserializes correctly" do
      {response_binary, expected} = AddOffsetsToTxnFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50
      assert response.error_code == 0
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, AddOffsetsToTxn, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: "txn",
            producer_id: 1,
            producer_epoch: 0,
            group_id: "group"
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, AddOffsetsToTxn, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: "txn",
            producer_id: 1,
            producer_epoch: 0,
            group_id: "group"
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
        {response_binary, _} = AddOffsetsToTxnFactory.response_data(version)
        response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])

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
        {response_binary, _} = AddOffsetsToTxnFactory.response_data(version)
        response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])

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
        response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 partial response (missing error_code) fails with FunctionClauseError" do
      # Only correlation_id and throttle_time_ms, missing error_code
      partial = <<0, 0, 0, 0, 0, 0, 0, 0>>

      assert_raise FunctionClauseError, fn ->
        Kayrock.AddOffsetsToTxn.V0.Response.deserialize(partial)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 51, "CONCURRENT_TRANSACTIONS"},
      {1, 47, "INVALID_PRODUCER_EPOCH"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.AddOffsetsToTxn, :"V#{version}", Response])

        response_binary = AddOffsetsToTxnFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        assert response.error_code == error_code
      end
    end
  end
end
