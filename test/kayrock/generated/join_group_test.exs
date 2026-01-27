defmodule Kayrock.JoinGroupTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.JoinGroupFactory

  # ============================================
  # Versions Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    test "all versions encode and decode correctly" do
      for version <- api_version_range(:join_group) do
        # Request encoding
        {request, expected_binary} = JoinGroupFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request encoding failed: #{compare_binaries(serialized, expected_binary)}"

        # Response decoding
        {response_binary, expected_struct} = JoinGroupFactory.response_data(version)
        response_module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Response])
        {actual, <<>>} = response_module.deserialize(response_binary)

        assert actual == expected_struct,
               "V#{version} response decoding failed"
      end
    end

    test "all available versions serialize" do
      for version <- api_version_range(:join_group) do
        module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          session_timeout_ms: 30_000,
          member_id: "",
          protocol_type: "consumer",
          protocols: []
        }

        fields =
          cond do
            version >= 6 ->
              Map.merge(base, %{
                rebalance_timeout_ms: 60_000,
                group_instance_id: nil,
                tagged_fields: []
              })

            version >= 5 ->
              Map.merge(base, %{
                rebalance_timeout_ms: 60_000,
                group_instance_id: nil
              })

            version >= 1 ->
              Map.put(base, :rebalance_timeout_ms, 60_000)

            true ->
              base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 11
        assert api_version == version
      end
    end
  end

  # ============================================
  # Specific Version Tests (V0 examples)
  # ============================================

  describe "V0" do
    alias Kayrock.JoinGroup.V0.Request
    alias Kayrock.JoinGroup.V0.Response

    test "deserializes leader response with members" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # generation_id
        0,
        0,
        0,
        2,
        # protocol_name
        0,
        5,
        "range"::binary,
        # leader (same as member_id for leader)
        0,
        8,
        "member-1"::binary,
        # member_id
        0,
        8,
        "member-1"::binary,
        # members array length (2 members)
        0,
        0,
        0,
        2,
        # member 1 id
        0,
        8,
        "member-1"::binary,
        # member 1 metadata
        0,
        0,
        0,
        4,
        1,
        2,
        3,
        4,
        # member 2 id
        0,
        8,
        "member-2"::binary,
        # member 2 metadata
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

      assert response.generation_id == 2
      assert response.leader == "member-1"
      assert length(response.members) == 2

      [member1, member2] = response.members
      assert member1.member_id == "member-1"
      assert member2.member_id == "member-2"
    end

    test "serializes request with GroupProtocolMetadata" do
      expect = <<
        # Preamble
        11::16,
        0::16,
        42::32,
        9::16,
        "client_id"::binary,
        # GroupId
        5::16,
        "group"::binary,
        # SessionTimeout
        3600::32,
        # MemberId
        9::16,
        "member_id"::binary,
        # ProtocolType
        8::16,
        "consumer"::binary,
        # GroupProtocols array size
        1::32,
        # Basic strategy
        6::16,
        "assign"::binary,
        # length of metadata
        32::32,
        # v0
        0::16,
        # Topics array
        2::32,
        9::16,
        "topic_one"::binary,
        9::16,
        "topic_two"::binary,
        # UserData
        0::32
      >>

      request = %Request{
        client_id: "client_id",
        correlation_id: 42,
        group_id: "group",
        protocols: [
          %{
            metadata:
              IO.iodata_to_binary(
                Kayrock.GroupProtocolMetadata.serialize(%Kayrock.GroupProtocolMetadata{
                  topics: ["topic_one", "topic_two"]
                })
              ),
            name: "assign"
          }
        ],
        member_id: "member_id",
        protocol_type: "consumer",
        session_timeout_ms: 3600
      }

      got = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert got == expect, compare_binaries(got, expect)
    end
  end

  # ============================================
  # V5 Static Membership Tests
  # ============================================

  describe "V5" do
    alias Kayrock.JoinGroup.V5.Request

    test "serializes request with nil group_instance_id" do
      request = %Request{
        correlation_id: 5,
        client_id: "test",
        group_id: "group",
        session_timeout_ms: 30_000,
        rebalance_timeout_ms: 60_000,
        member_id: "",
        group_instance_id: nil,
        protocol_type: "consumer",
        protocols: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "edge cases" do
    alias Kayrock.JoinGroup.V0.Request
    alias Kayrock.JoinGroup.V0.Response

    test "empty member_id (initial join) serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        session_timeout_ms: 30_000,
        member_id: "",
        protocol_type: "consumer",
        protocols: [
          %{name: "range", metadata: <<>>}
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "multiple protocols (range and roundrobin) serializes correctly" do
      range_metadata =
        IO.iodata_to_binary(
          Kayrock.GroupProtocolMetadata.serialize(%Kayrock.GroupProtocolMetadata{
            topics: ["topic-1", "topic-2"]
          })
        )

      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        session_timeout_ms: 30_000,
        member_id: "",
        protocol_type: "consumer",
        protocols: [
          %{name: "range", metadata: range_metadata},
          %{name: "roundrobin", metadata: range_metadata}
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "range")
      assert String.contains?(serialized, "roundrobin")
    end

    test "many members in leader response deserializes correctly" do
      # Build response with 5 members
      members_binary =
        for i <- 1..5, into: <<>> do
          member_id = "member-#{i}"
          <<byte_size(member_id)::16, member_id::binary, 0, 0, 0, 0>>
        end

      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # generation_id
        0,
        0,
        0,
        1,
        # protocol_name
        0,
        5,
        "range"::binary,
        # leader
        0,
        8,
        "member-1"::binary,
        # member_id
        0,
        8,
        "member-1"::binary,
        # members array length
        0,
        0,
        0,
        5,
        members_binary::binary
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert length(response.members) == 5
    end

    test "long group_id serializes correctly" do
      long_group_id = String.duplicate("g", 200)

      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: long_group_id,
        session_timeout_ms: 30_000,
        member_id: "",
        protocol_type: "consumer",
        protocols: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, long_group_id)
    end
  end

  # ============================================
  # Error Response Tests
  # ============================================

  describe "error responses" do
    test "all versions deserialize error responses correctly" do
      error_codes = [
        {25, "UNKNOWN_MEMBER_ID"},
        {27, "REBALANCE_IN_PROGRESS"},
        {15, "COORDINATOR_NOT_AVAILABLE"}
      ]

      for version <- api_version_range(:join_group),
          {error_code, _error_name} <- error_codes do
        response_binary = JoinGroupFactory.error_response(version, error_code: error_code)
        response_module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Response])
        {response, <<>>} = response_module.deserialize(response_binary)

        assert response.error_code == error_code,
               "V#{version} should deserialize error_code #{error_code}"

        assert response.generation_id == -1,
               "V#{version} should have generation_id -1 on error"
      end
    end
  end

  # ============================================
  # Malformed Input Tests
  # ============================================

  describe "malformed inputs" do
    test "truncated binaries fail for all versions" do
      for version <- api_version_range(:join_group) do
        {response_binary, _expected} = JoinGroupFactory.response_data(version)
        response_module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end

    test "extra trailing bytes are returned for all versions" do
      for version <- api_version_range(:join_group) do
        {response_binary, _expected} = JoinGroupFactory.response_data(version)
        response_module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Response])
        assert_extra_bytes_returned(response_module, response_binary, <<99, 100>>)
      end
    end

    test "empty binary fails for all versions" do
      for version <- api_version_range(:join_group) do
        response_module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "invalid members array length fails" do
      invalid = <<
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        0,
        2,
        0,
        5,
        "range"::binary,
        0,
        8,
        "member-1"::binary,
        0,
        8,
        "member-1"::binary,
        # Claims 1000 members but no data
        0,
        0,
        3,
        232
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.JoinGroup.V0.Response.deserialize(invalid)
      end
    end
  end
end
