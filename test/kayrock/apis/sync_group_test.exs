defmodule Kayrock.Apis.SyncGroupTest do
  @moduledoc """
  Tests for SyncGroup API (V0-V5).

  API Key: 14
  Used to: Distribute partition assignments to group members after a rebalance.

  Protocol structure:
  - V0: Basic sync with assignments
  - V1+: Adds throttle_time in response
  - V3+: Adds group_instance_id for static membership
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.SyncGroup.V0.Request
    alias Kayrock.SyncGroup.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        generation_id: 1,
        member_id: "member-123",
        assignments: []
      }

      expected = <<
        # api_key (14 = SyncGroup)
        0,
        14,
        # api_version (0)
        0,
        0,
        # correlation_id
        0,
        0,
        0,
        0,
        # client_id
        0,
        4,
        "test"::binary,
        # group_id
        0,
        8,
        "my-group"::binary,
        # generation_id
        0,
        0,
        0,
        1,
        # member_id
        0,
        10,
        "member-123"::binary,
        # assignments (empty)
        0,
        0,
        0,
        0
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    test "serializes request with assignments" do
      assignment_data =
        <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        generation_id: 2,
        member_id: "leader-id",
        assignments: [
          %{
            member_id: "member-1",
            assignment: assignment_data
          },
          %{
            member_id: "member-2",
            assignment: assignment_data
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 14
      assert api_version == 0
    end

    test "deserializes response" do
      # Assignment is a MemberAssignment struct: version (int16) + assignments array + user_data
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (0 = no error)
        0,
        0,
        # assignment length (10 bytes)
        0,
        0,
        0,
        10,
        # version (0)
        0,
        0,
        # partition_assignments (empty array)
        0,
        0,
        0,
        0,
        # user_data (empty bytes)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert response.assignment.version == 0
      assert response.assignment.partition_assignments == []
    end

    test "deserializes response with empty assignment" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # assignment length
        0,
        0,
        0,
        10,
        # version
        0,
        0,
        # partition_assignments (empty)
        0,
        0,
        0,
        0,
        # user_data (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.error_code == 0
      assert %Kayrock.MemberAssignment{} = response.assignment
    end

    test "deserializes error response (rebalance in progress)" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # error_code (27 = REBALANCE_IN_PROGRESS)
        0,
        27,
        # assignment length
        0,
        0,
        0,
        10,
        # version
        0,
        0,
        # partition_assignments (empty)
        0,
        0,
        0,
        0,
        # user_data (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 27
    end
  end

  describe "V1" do
    alias Kayrock.SyncGroup.V1.Request
    alias Kayrock.SyncGroup.V1.Response

    test "serializes request (same as V0)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        assignments: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 14
      assert api_version == 1
    end

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms (100)
        0,
        0,
        0,
        100,
        # error_code
        0,
        0,
        # assignment (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.throttle_time_ms == 100
      assert response.error_code == 0
    end
  end

  describe "V3" do
    alias Kayrock.SyncGroup.V3.Request

    test "serializes request with group_instance_id" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        group_instance_id: "static-instance",
        assignments: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 14
      assert api_version == 3
    end

    test "serializes request with nil group_instance_id" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        group_id: "group",
        generation_id: 1,
        member_id: "member",
        group_instance_id: nil,
        assignments: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:sync_group) do
        module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          generation_id: 1,
          member_id: "member",
          assignments: []
        }

        fields =
          if version >= 3 do
            Map.put(base, :group_instance_id, nil)
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 14
        assert api_version == version
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "edge cases" do
    alias Kayrock.SyncGroup.V0.Request
    alias Kayrock.SyncGroup.V0.Response

    test "leader with many assignments serializes correctly" do
      assignment_data =
        <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

      assignments =
        for i <- 1..10 do
          %{member_id: "member-#{i}", assignment: assignment_data}
        end

      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        generation_id: 5,
        member_id: "leader",
        assignments: assignments
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      assert String.contains?(serialized, "member-1")
      assert String.contains?(serialized, "member-10")
    end

    test "non-leader (empty assignments) serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        generation_id: 5,
        member_id: "follower",
        assignments: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "response with UNKNOWN_MEMBER_ID error deserializes" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (25 = UNKNOWN_MEMBER_ID)
        0,
        25,
        # assignment length
        0,
        0,
        0,
        10,
        # version
        0,
        0,
        # partition_assignments (empty)
        0,
        0,
        0,
        0,
        # user_data (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 25
    end

    test "response with ILLEGAL_GENERATION error deserializes" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (22 = ILLEGAL_GENERATION)
        0,
        22,
        # assignment length
        0,
        0,
        0,
        10,
        # version
        0,
        0,
        # partition_assignments (empty)
        0,
        0,
        0,
        0,
        # user_data (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 22
    end
  end
end
