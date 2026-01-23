defmodule Kayrock.Apis.JoinGroupTest do
  @moduledoc """
  Tests for JoinGroup API (V0-V7).

  API Key: 11
  Used to: Join a consumer group and get initial membership assignment.

  Protocol structure:
  - V0: Basic join with session_timeout
  - V1+: Adds rebalance_timeout
  - V5+: Adds group_instance_id for static membership
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.JoinGroup.V0.Request
    alias Kayrock.JoinGroup.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        session_timeout_ms: 30000,
        member_id: "",
        protocol_type: "consumer",
        protocols: [
          %{
            name: "range",
            metadata: <<0, 1, 0, 0, 0, 1, 0, 5, "topic"::binary, 0, 0, 0, 0>>
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 11
      assert api_version == 0
    end

    test "deserializes response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (0 = no error)
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
        6,
        "leader"::binary,
        # member_id
        0,
        9,
        "member-id"::binary,
        # members array (empty for non-leader)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert response.generation_id == 1
      assert response.protocol_name == "range"
      assert response.leader == "leader"
      assert response.member_id == "member-id"
      assert response.members == []
    end

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

    test "deserializes error response (unknown member)" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # error_code (25 = UNKNOWN_MEMBER_ID)
        0,
        25,
        # generation_id (-1)
        255,
        255,
        255,
        255,
        # protocol_name (empty)
        0,
        0,
        # leader (empty)
        0,
        0,
        # member_id (empty)
        0,
        0,
        # members (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 25
      assert response.generation_id == -1
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

    test "serializes request with preserialized protocol metadata" do
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
              <<0::16, 2::32, 9::16, "topic_one"::binary, 9::16, "topic_two"::binary, 0::32>>,
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

  describe "V1" do
    alias Kayrock.JoinGroup.V1.Request

    test "serializes request with rebalance_timeout" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        member_id: "",
        protocol_type: "consumer",
        protocols: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 11
      assert api_version == 1
    end
  end

  describe "V5" do
    alias Kayrock.JoinGroup.V5.Request

    test "serializes request with group_instance_id" do
      request = %Request{
        correlation_id: 5,
        client_id: "test",
        group_id: "group",
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        member_id: "",
        group_instance_id: "static-instance-1",
        protocol_type: "consumer",
        protocols: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 11
      assert api_version == 5
    end

    test "serializes request with nil group_instance_id" do
      request = %Request{
        correlation_id: 5,
        client_id: "test",
        group_id: "group",
        session_timeout_ms: 30000,
        rebalance_timeout_ms: 60000,
        member_id: "",
        group_instance_id: nil,
        protocol_type: "consumer",
        protocols: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:join_group) do
        module = Module.concat([Kayrock, JoinGroup, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          session_timeout_ms: 30000,
          member_id: "",
          protocol_type: "consumer",
          protocols: []
        }

        fields =
          cond do
            version >= 5 ->
              Map.merge(base, %{
                rebalance_timeout_ms: 60000,
                group_instance_id: nil
              })

            version >= 1 ->
              Map.put(base, :rebalance_timeout_ms, 60000)

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

    test "response with REBALANCE_IN_PROGRESS error deserializes" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (27 = REBALANCE_IN_PROGRESS)
        0,
        27,
        # generation_id (-1)
        255,
        255,
        255,
        255,
        # protocol_name (empty)
        0,
        0,
        # leader (empty)
        0,
        0,
        # member_id
        0,
        9,
        "member-id"::binary,
        # members (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 27
    end

    test "response with COORDINATOR_NOT_AVAILABLE error deserializes" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (15 = COORDINATOR_NOT_AVAILABLE)
        0,
        15,
        # generation_id (-1)
        255,
        255,
        255,
        255,
        # protocol_name (empty)
        0,
        0,
        # leader (empty)
        0,
        0,
        # member_id (empty)
        0,
        0,
        # members (empty)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 15
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
end
