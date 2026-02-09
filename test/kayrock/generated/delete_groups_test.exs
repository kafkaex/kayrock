defmodule Kayrock.DeleteGroupsTest do
  @moduledoc """
  Tests for DeleteGroups API (V0-V2).

  API Key: 42
  Used to: Delete consumer groups from the Kafka cluster.

  Protocol structure:
  - V0-V1: Basic delete with throttle_time_ms in response
  - V2: Flexible/compact format with tagged_fields
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DeleteGroupsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..2 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = DeleteGroupsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 42
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = DeleteGroupsFactory.response_data(version)

        response_module = Module.concat([Kayrock.DeleteGroups, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.results)
      end
    end

    test "all available versions have modules" do
      for version <- 0..2 do
        request_module = Module.concat([Kayrock, DeleteGroups, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, DeleteGroups, :"V#{version}", Response])

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

  describe "V0-V1 - basic delete" do
    test "request with multiple groups serializes correctly" do
      request = %Kayrock.DeleteGroups.V0.Request{
        correlation_id: 1,
        client_id: "test",
        groups_names: ["group1", "group2", "group3"]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 42
      assert api_version == 0
    end

    test "response with multiple groups deserializes correctly" do
      response_binary = DeleteGroupsFactory.multi_group_response(0)
      {response, <<>>} = Kayrock.DeleteGroups.V0.Response.deserialize(response_binary)

      assert length(response.results) == 2
      [success, error] = response.results
      assert success.group_id == "group1"
      assert success.error_code == 0
      assert error.group_id == "group2"
      assert error.error_code == 69
    end
  end

  describe "V2 - flexible/compact format" do
    alias Kayrock.DeleteGroups.V2.Response

    test "deserializes response with tagged_fields" do
      {response_binary, expected_struct} = DeleteGroupsFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.tagged_fields == []

      [result] = response.results
      assert result.group_id == "group1"
      assert result.error_code == 0
      assert result.tagged_fields == []
    end

    test "deserializes multi-group response with V2 fields" do
      response_binary = DeleteGroupsFactory.multi_group_response(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert length(response.results) == 2
      [success, error] = response.results

      assert success.group_id == "group1"
      assert success.error_code == 0
      assert error.group_id == "group2"
      assert error.error_code == 69
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, DeleteGroups, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test")

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, DeleteGroups, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test")

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
        {response_binary, _} = DeleteGroupsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteGroups, :"V#{version}", Response])

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
        {response_binary, _} = DeleteGroupsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteGroups, :"V#{version}", Response])

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
        response_module = Module.concat([Kayrock.DeleteGroups, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 invalid results array length fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # Claims 500 results (but no data follows)
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DeleteGroups.V0.Response.deserialize(invalid)
      end
    end

    test "V2 invalid compact array fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # header tagged_fields
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # results compact array with huge value (254 results, but no data)
        255
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DeleteGroups.V2.Response.deserialize(invalid)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 69, "GROUP_ID_NOT_FOUND"},
      {1, 30, "GROUP_AUTHORIZATION_FAILED"},
      {2, 16, "NOT_COORDINATOR"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.DeleteGroups, :"V#{version}", Response])

        response_binary = DeleteGroupsFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [result] = response.results
        assert result.error_code == error_code
      end
    end
  end
end
