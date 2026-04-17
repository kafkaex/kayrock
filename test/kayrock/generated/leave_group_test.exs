defmodule Kayrock.LeaveGroupTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.LeaveGroupFactory

  # ============================================
  # Version Compatibility Tests (All Versions)
  # ============================================

  describe "versions compatibility" do
    test "all versions serialize requests correctly" do
      for version <- api_version_range(:leave_group) do
        {request, expected_binary} = LeaveGroupFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary, "V#{version} request serialization failed"
      end
    end

    test "all versions deserialize responses correctly" do
      for version <- api_version_range(:leave_group) do
        response_module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Response])
        {response_binary, expected_struct} = LeaveGroupFactory.response_data(version)
        {actual, <<>>} = response_module.deserialize(response_binary)
        assert actual == expected_struct, "V#{version} response deserialization failed"
      end
    end

    test "all versions round-trip correctly" do
      for version <- api_version_range(:leave_group) do
        {request, expected_binary} = LeaveGroupFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        <<api_key::16, api_version::16, _rest::binary>> = serialized

        assert api_key == 13, "V#{version} wrong API key"
        assert api_version == version, "V#{version} wrong API version"
        assert serialized == expected_binary, "V#{version} round-trip failed"
      end
    end
  end

  # ============================================
  # Version-Specific Feature Tests
  # ============================================

  describe "V0 specific" do
    alias Kayrock.LeaveGroup.V0.Response

    test "deserializes error response (unknown member)" do
      response_binary = LeaveGroupFactory.response_binary(0, error_code: 25, correlation_id: 1)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 25
      assert response.correlation_id == 1
    end
  end

  describe "V1+ throttle_time_ms" do
    test "V1 includes throttle_time_ms" do
      {_binary, expected_struct} = LeaveGroupFactory.response_data(1)
      assert expected_struct.throttle_time_ms == 50
    end

    test "V2 includes throttle_time_ms" do
      {_binary, expected_struct} = LeaveGroupFactory.response_data(2)
      assert expected_struct.throttle_time_ms == 50
    end
  end

  describe "V3 batch members" do
    alias Kayrock.LeaveGroup.V3.Request
    alias Kayrock.LeaveGroup.V3.Response

    test "serializes request with multiple members" do
      {request, expected_binary} = LeaveGroupFactory.request_data(3)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary
      assert length(request.members) == 2
    end

    test "deserializes response with member-level errors" do
      {response_binary, expected_struct} = LeaveGroupFactory.response_data(3)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert length(response.members) == 2

      [member1, member2] = response.members
      assert member1.member_id == "member-1"
      assert member1.group_instance_id == nil
      assert member1.error_code == 0

      assert member2.member_id == "member-2"
      assert member2.group_instance_id == "static-1"
      assert member2.error_code == 25
    end

    test "serializes request with empty members array" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        members: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      <<_header::14-binary, _group_id::7-binary, members_count::32-signed, _rest::binary>> =
        serialized

      assert members_count == 0
    end
  end

  describe "V4 flexible format" do
    alias Kayrock.LeaveGroup.V4.Request
    alias Kayrock.LeaveGroup.V4.Response

    test "serializes request with tagged fields" do
      {request, expected_binary} = LeaveGroupFactory.request_data(4)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary
      assert request.tagged_fields == []
    end

    test "deserializes response with tagged fields" do
      {response_binary, expected_struct} = LeaveGroupFactory.response_data(4)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.tagged_fields == []
    end

    test "uses compact string encoding" do
      request = %Request{
        correlation_id: 4,
        client_id: "test",
        group_id: "abc",
        members: [%{member_id: "m", group_instance_id: nil, tagged_fields: []}],
        tagged_fields: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      # After header: api_key(2) + api_version(2) + correlation_id(4) +
      # int16-prefixed client_id (2 prefix + "test"=4 = 6) + tag_buffer(1) = 15
      <<_header::15-binary, group_id_length, "abc", _rest::binary>> = serialized
      # length("abc") + 1
      assert group_id_length == 4
    end
  end

  # ============================================
  # Edge Case Tests (All Versions)
  # ============================================

  describe "truncated binary handling" do
    test "all versions handle truncated responses" do
      for version <- api_version_range(:leave_group) do
        response_module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Response])
        {response_binary, _expected} = LeaveGroupFactory.response_data(version)

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    test "all versions return extra trailing bytes" do
      for version <- api_version_range(:leave_group) do
        response_module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Response])
        {response_binary, _expected} = LeaveGroupFactory.response_data(version)

        assert_extra_bytes_returned(response_module, response_binary, <<11, 22, 33>>)
      end
    end
  end

  describe "malformed response handling" do
    test "all versions fail on empty binary" do
      for version <- api_version_range(:leave_group) do
        response_module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "all versions fail on partial response" do
      for version <- api_version_range(:leave_group) do
        response_module = Module.concat([Kayrock, LeaveGroup, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<0, 0>>)
        end
      end
    end
  end

  describe "custom error scenarios" do
    test "V0 handles various error codes" do
      alias Kayrock.LeaveGroup.V0.Response

      for error_code <- [0, 25, 15, 16] do
        binary = LeaveGroupFactory.error_response(0, error_code: error_code, correlation_id: 99)
        {response, <<>>} = Response.deserialize(binary)
        assert response.error_code == error_code
        assert response.correlation_id == 99
      end
    end

    test "V1+ handles throttle with errors" do
      alias Kayrock.LeaveGroup.V1.Response

      binary =
        LeaveGroupFactory.response_binary(1,
          error_code: 25,
          throttle_time_ms: 200,
          correlation_id: 42
        )

      {response, <<>>} = Response.deserialize(binary)
      assert response.error_code == 25
      assert response.throttle_time_ms == 200
      assert response.correlation_id == 42
    end

    test "V3 handles empty members array" do
      alias Kayrock.LeaveGroup.V3.Response

      binary =
        LeaveGroupFactory.response_binary(3,
          correlation_id: 5,
          throttle_time_ms: 0,
          error_code: 0,
          members: []
        )

      {response, <<>>} = Response.deserialize(binary)
      assert response.members == []
      assert response.error_code == 0
    end
  end
end
