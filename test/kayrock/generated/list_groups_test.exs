defmodule Kayrock.ListGroupsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.ListGroupsFactory

  # ============================================
  # Request Serialization Tests
  # ============================================

  describe "request serialization" do
    for version <- 0..3 do
      test "V#{version} serializes correctly" do
        version = unquote(version)
        {request, expected_binary} = ListGroupsFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary
      end
    end
  end

  # ============================================
  # Response Deserialization Tests
  # ============================================

  describe "response deserialization" do
    for version <- 0..3 do
      test "V#{version} deserializes correctly" do
        version = unquote(version)
        {response_binary, expected_struct} = ListGroupsFactory.response_data(version)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        {actual, <<>>} = response_module.deserialize(response_binary)

        assert actual == expected_struct
      end
    end

    test "V0 deserializes empty groups" do
      response_binary = ListGroupsFactory.empty_response(0, correlation_id: 1)
      {response, <<>>} = Kayrock.ListGroups.V0.Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.error_code == 0
      assert response.groups == []
    end

    test "V0 deserializes error response" do
      response_binary = ListGroupsFactory.error_response(0, correlation_id: 2, error_code: 29)
      {response, <<>>} = Kayrock.ListGroups.V0.Response.deserialize(response_binary)

      assert response.correlation_id == 2
      assert response.error_code == 29
      assert response.groups == []
    end
  end

  # ============================================
  # Version Compatibility
  # ============================================

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:list_groups) do
        request_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Request])

        assert Code.ensure_loaded?(request_module),
               "Module #{inspect(request_module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test"
        }

        fields =
          if version >= 3 do
            Map.put(base, :tagged_fields, [])
          else
            base
          end

        request = struct(request_module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 16
        assert api_version == version
      end
    end

    test "all available versions deserialize" do
      for version <- api_version_range(:list_groups) do
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        assert Code.ensure_loaded?(response_module),
               "Module #{inspect(response_module)} should exist"

        {response_binary, expected_struct} = ListGroupsFactory.response_data(version)
        {actual, <<>>} = response_module.deserialize(response_binary)

        assert actual == expected_struct, "V#{version} should deserialize correctly"
      end
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..3 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _expected} = ListGroupsFactory.response_data(version)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    for version <- 0..3 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _expected} = ListGroupsFactory.response_data(version)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<88, 77, 66>>)
      end
    end
  end

  describe "malformed response handling" do
    for version <- 0..3 do
      test "V#{version} empty binary fails" do
        version = unquote(version)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 invalid groups array length fails" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # Claims 1000 groups but provides no data
        0,
        0,
        3,
        232
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.ListGroups.V0.Response.deserialize(invalid)
      end
    end
  end

  # ============================================
  # Helper Functions
  # ============================================

  describe "response helpers" do
    test "response_binary/2 creates valid responses for all versions" do
      for version <- 0..3 do
        custom_binary =
          ListGroupsFactory.response_binary(version,
            correlation_id: 99,
            error_code: 0,
            groups: [
              %{group_id: "custom-group", protocol_type: "custom-protocol"}
            ]
          )

        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])
        {response, <<>>} = response_module.deserialize(custom_binary)

        assert response.correlation_id == 99
        assert response.error_code == 0
        assert length(response.groups) == 1

        [group] = response.groups
        assert group.group_id == "custom-group"
        assert group.protocol_type == "custom-protocol"
      end
    end

    test "empty_response/1 creates valid empty responses" do
      for version <- 0..3 do
        empty_binary = ListGroupsFactory.empty_response(version)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        {response, <<>>} = response_module.deserialize(empty_binary)
        assert response.groups == []
      end
    end

    test "error_response/1 creates valid error responses" do
      for version <- 0..3 do
        error_binary = ListGroupsFactory.error_response(version, error_code: 15)
        response_module = Module.concat([Kayrock, ListGroups, :"V#{version}", Response])

        {response, <<>>} = response_module.deserialize(error_binary)
        assert response.error_code == 15
        assert response.groups == []
      end
    end
  end
end
