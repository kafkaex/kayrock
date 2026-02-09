defmodule Kayrock.DescribeGroupsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DescribeGroupsFactory

  # ============================================
  # Version Compatibility Tests (All Versions)
  # ============================================

  describe "versions compatibility" do
    test "all versions serialize and deserialize" do
      for version <- api_version_range(:describe_groups) do
        # Test request serialization
        {request, expected_binary} = DescribeGroupsFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request serialization failed"

        # Verify API key and version in serialized data
        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 15
        assert api_version == version

        # Test response deserialization
        {response_binary, expected_struct} = DescribeGroupsFactory.response_data(version)
        {actual_struct, <<>>} = Kayrock.DescribeGroups.deserialize(version, response_binary)

        assert actual_struct == expected_struct,
               "V#{version} response deserialization failed"
      end
    end
  end

  # ============================================
  # Specific Version Tests
  # ============================================

  describe "V0" do
    alias Kayrock.DescribeGroups.V0.Request
    alias Kayrock.DescribeGroups.V0.Response

    test "serializes request with multiple groups" do
      {request, expected_binary} = DescribeGroupsFactory.request_data(0)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "serializes request with single group" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        groups: ["my-group"]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "deserializes response" do
      {response_binary, expected_struct} = DescribeGroupsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response == expected_struct
    end

    test "deserializes response with members" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # groups
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # group_id
        0,
        5,
        "group"::binary,
        # state
        0,
        6,
        "Stable"::binary,
        # protocol_type
        0,
        8,
        "consumer"::binary,
        # protocol
        0,
        5,
        "range"::binary,
        # members array
        0,
        0,
        0,
        1,
        # member_id
        0,
        6,
        "member"::binary,
        # client_id
        0,
        6,
        "client"::binary,
        # client_host
        0,
        9,
        "localhost"::binary,
        # member_metadata
        0,
        0,
        0,
        4,
        1,
        2,
        3,
        4,
        # member_assignment
        0,
        0,
        0,
        4,
        5,
        6,
        7,
        8
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [group] = response.groups
      [member] = group.members
      assert member.member_id == "member"
      assert member.client_id == "client"
      assert member.client_host == "localhost"
      assert member.member_metadata == <<1, 2, 3, 4>>
      assert member.member_assignment == <<5, 6, 7, 8>>
    end

    test "deserializes error response" do
      response_binary = DescribeGroupsFactory.error_response(0, correlation_id: 2)
      {response, <<>>} = Response.deserialize(response_binary)

      [group] = response.groups
      assert group.error_code == 16
    end
  end

  describe "V1" do
    alias Kayrock.DescribeGroups.V1.Response

    test "deserializes response with throttle_time" do
      {response_binary, expected_struct} = DescribeGroupsFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response == expected_struct
      assert response.throttle_time_ms == 100
    end
  end

  describe "V3" do
    alias Kayrock.DescribeGroups.V3.Request
    alias Kayrock.DescribeGroups.V3.Response

    test "serializes request with include_authorized_operations" do
      {request, expected_binary} = DescribeGroupsFactory.request_data(3)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "deserializes response with authorized_operations" do
      {response_binary, expected_struct} = DescribeGroupsFactory.response_data(3)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response == expected_struct
    end
  end

  describe "V4" do
    alias Kayrock.DescribeGroups.V4.Response

    test "deserializes response with group_instance_id" do
      {response_binary, expected_struct} = DescribeGroupsFactory.response_data(4)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response == expected_struct

      [group] = response.groups
      [member] = group.members
      assert member.group_instance_id == nil
    end
  end

  describe "V5 (flexible format)" do
    alias Kayrock.DescribeGroups.V5.Request
    alias Kayrock.DescribeGroups.V5.Response

    test "serializes request with compact format" do
      {request, expected_binary} = DescribeGroupsFactory.request_data(5)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "deserializes response with tagged fields" do
      {response_binary, expected_struct} = DescribeGroupsFactory.response_data(5)
      {response, <<>>} = Response.deserialize(response_binary)
      assert response == expected_struct
      assert response.tagged_fields == []
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "all versions handle truncated binary" do
      for version <- api_version_range(:describe_groups) do
        response_binary = DescribeGroupsFactory.captured_response_binary(version)
        response_module = Module.concat([Kayrock, DescribeGroups, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    test "all versions return extra trailing bytes" do
      for version <- api_version_range(:describe_groups) do
        response_binary = DescribeGroupsFactory.captured_response_binary(version)
        extra_bytes = <<77, 88>>
        response_module = Module.concat([Kayrock, DescribeGroups, :"V#{version}", Response])

        {_response, rest} = response_module.deserialize(response_binary <> extra_bytes)
        assert rest == extra_bytes
      end
    end
  end

  describe "malformed response handling" do
    test "empty binary fails" do
      assert_raise MatchError, fn ->
        Kayrock.DescribeGroups.V0.Response.deserialize(<<>>)
      end
    end

    test "invalid groups array length fails" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # Claims 500 groups
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DescribeGroups.V0.Response.deserialize(invalid)
      end
    end

    test "partial correlation_id fails" do
      invalid = <<0, 0>>

      assert_raise MatchError, fn ->
        Kayrock.DescribeGroups.V0.Response.deserialize(invalid)
      end
    end
  end

  describe "error response scenarios" do
    test "all versions handle NOT_COORDINATOR error" do
      for version <- api_version_range(:describe_groups) do
        error_binary = DescribeGroupsFactory.error_response(version, error_code: 16)
        response_module = Module.concat([Kayrock, DescribeGroups, :"V#{version}", Response])

        {response, <<>>} = response_module.deserialize(error_binary)
        [group] = response.groups
        assert group.error_code == 16
      end
    end
  end
end
