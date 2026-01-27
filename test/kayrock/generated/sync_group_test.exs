defmodule Kayrock.SyncGroupTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.SyncGroupFactory

  # ============================================
  # Request Serialization Tests (Factory-based)
  # ============================================

  describe "request serialization" do
    test "all versions serialize correctly" do
      for version <- api_version_range(:sync_group) do
        {request, expected_binary} = SyncGroupFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request serialization failed.\n" <>
                 "Expected: #{inspect(expected_binary, limit: :infinity)}\n" <>
                 "Got: #{inspect(serialized, limit: :infinity)}"
      end
    end
  end

  # ============================================
  # Response Deserialization Tests (Factory-based)
  # ============================================

  describe "response deserialization" do
    test "all versions deserialize correctly" do
      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])
        {response_binary, expected_struct} = SyncGroupFactory.response_data(version)

        {actual, rest} = response_module.deserialize(response_binary)

        assert actual == expected_struct,
               "V#{version} response deserialization failed.\n" <>
                 "Expected: #{inspect(expected_struct, pretty: true)}\n" <>
                 "Got: #{inspect(actual, pretty: true)}"

        assert rest == <<>>, "Expected no remaining bytes, got: #{inspect(rest)}"
      end
    end
  end

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
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
          cond do
            version >= 4 ->
              base
              |> Map.put(:group_instance_id, nil)
              |> Map.put(:tagged_fields, [])

            version >= 3 ->
              Map.put(base, :group_instance_id, nil)

            true ->
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

    test "all available responses deserialize" do
      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])

        assert Code.ensure_loaded?(response_module),
               "Module #{inspect(response_module)} should exist"

        response_binary = SyncGroupFactory.captured_response_binary(version)
        {response, rest} = response_module.deserialize(response_binary)

        assert %{correlation_id: _} = response
        assert rest == <<>>
      end
    end
  end

  # ============================================
  # Edge Cases - Assignments
  # ============================================

  describe "edge cases - assignments" do
    test "request with multiple assignments serializes correctly" do
      assignment_data =
        <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

      request = %Kayrock.SyncGroup.V0.Request{
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

    test "leader with many assignments serializes correctly" do
      assignment_data =
        <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

      assignments =
        for i <- 1..10 do
          %{member_id: "member-#{i}", assignment: assignment_data}
        end

      request = %Kayrock.SyncGroup.V0.Request{
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
      request = %Kayrock.SyncGroup.V0.Request{
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
  end

  # ============================================
  # Edge Cases - group_instance_id (V3+)
  # ============================================

  describe "edge cases - group_instance_id" do
    test "V3 request with non-null group_instance_id serializes" do
      request = %Kayrock.SyncGroup.V3.Request{
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

    test "V3 request with nil group_instance_id serializes" do
      request = %Kayrock.SyncGroup.V3.Request{
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

  # ============================================
  # Edge Cases - Error Responses
  # ============================================

  describe "edge cases - error responses" do
    test "error responses deserialize for all versions" do
      error_codes = [
        {22, "ILLEGAL_GENERATION"},
        {25, "UNKNOWN_MEMBER_ID"},
        {27, "REBALANCE_IN_PROGRESS"}
      ]

      for version <- api_version_range(:sync_group),
          {error_code, _name} <- error_codes do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])
        response_binary = SyncGroupFactory.error_response(version, error_code: error_code)

        {response, rest} = response_module.deserialize(response_binary)

        assert response.error_code == error_code,
               "V#{version} should deserialize error_code #{error_code}"

        assert rest == <<>>, "V#{version} should consume all bytes"
      end
    end
  end

  # ============================================
  # Truncated Binary Handling
  # ============================================

  describe "truncated binary handling" do
    test "all versions reject truncated binaries" do
      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])
        response_binary = SyncGroupFactory.captured_response_binary(version)

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  # ============================================
  # Extra Bytes Handling
  # ============================================

  describe "extra bytes handling" do
    test "all versions return extra trailing bytes" do
      extra_bytes = <<44, 55, 66>>

      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])
        response_binary = SyncGroupFactory.captured_response_binary(version)

        assert_extra_bytes_returned(response_module, response_binary, extra_bytes)
      end
    end
  end

  # ============================================
  # Malformed Response Handling
  # ============================================

  describe "malformed response handling" do
    test "empty binary fails for all versions" do
      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "partial response fails for all versions" do
      for version <- api_version_range(:sync_group) do
        response_module = Module.concat([Kayrock, SyncGroup, :"V#{version}", Response])

        assert_raise FunctionClauseError, fn ->
          response_module.deserialize(<<0, 0, 0, 0, 0>>)
        end
      end
    end
  end
end
