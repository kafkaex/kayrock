defmodule Kayrock.DeleteAclsTest do
  @moduledoc """
  Tests for DeleteAcls API (V0-V1).

  API Key: 31
  Used to: Delete ACLs matching filter criteria.

  Protocol structure:
  - V0: filters array with resource_type, resource_name, principal, host, operation, permission_type
  - V1: Adds resource_pattern_type_filter to filters
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DeleteAclsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = DeleteAclsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 31
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = DeleteAclsFactory.response_data(version)

        response_module = Module.concat([Kayrock.DeleteAcls, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.filter_responses)
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, DeleteAcls, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, DeleteAcls, :"V#{version}", Response])

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

  describe "V0 - basic delete ACLs" do
    alias Kayrock.DeleteAcls.V0.Request
    alias Kayrock.DeleteAcls.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        filters: [
          %{
            resource_type: 2,
            resource_name: "my-topic",
            principal: "User:test",
            host: "*",
            operation: 2,
            permission_type: 3
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 31
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = DeleteAclsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0

      [filter_response] = response.filter_responses
      assert filter_response.error_code == 0

      [matching_acl] = filter_response.matching_acls
      assert matching_acl.resource_type == 2
      assert matching_acl.resource_name == "topic1"
    end
  end

  describe "V1 - adds resource_pattern_type" do
    alias Kayrock.DeleteAcls.V1.Request
    alias Kayrock.DeleteAcls.V1.Response

    test "request with resource_pattern_type_filter serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        filters: [
          %{
            resource_type: 2,
            resource_name: "topic-prefix",
            resource_pattern_type_filter: 3,
            principal: "User:admin",
            host: "192.168.1.1",
            operation: 3,
            permission_type: 3
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 31
      assert api_version == 1
    end

    test "response with resource_pattern_type deserializes correctly" do
      {response_binary, expected} = DeleteAclsFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50

      [filter_response] = response.filter_responses
      [matching_acl] = filter_response.matching_acls
      assert matching_acl.resource_pattern_type == 3
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, DeleteAcls, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            filters: []
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, DeleteAcls, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            filters: []
          )

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..1 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = DeleteAclsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteAcls, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    for version <- 0..1 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = DeleteAclsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteAcls, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  describe "malformed response handling" do
    for version <- 0..1 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.DeleteAcls, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  describe "error code handling" do
    @error_codes [
      {0, 7, "REQUEST_TIMED_OUT"},
      {1, 29, "CLUSTER_AUTHORIZATION_FAILED"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.DeleteAcls, :"V#{version}", Response])

        response_binary = DeleteAclsFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [filter_response] = response.filter_responses
        assert filter_response.error_code == error_code
        assert filter_response.error_message == "Delete ACL failed"
      end
    end
  end
end
