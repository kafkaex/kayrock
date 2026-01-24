defmodule Kayrock.Apis.OffsetCommitTest do
  @moduledoc """
  Tests for OffsetCommit API (V0-V8).

  API Key: 8
  Used to: Commit consumer offsets to Kafka.

  Protocol structure:
  - V0: Basic offset commit
  - V1+: Adds generation_id, member_id
  - V2+: Adds retention_time
  - V5+: Compact format
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.OffsetCommit.V0.Request
    alias Kayrock.OffsetCommit.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        topics: [
          %{
            name: "topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_metadata: ""
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 8
      assert api_version == 0
    end

    test "deserializes response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # topics array length
        0,
        0,
        0,
        1,
        # topic name
        0,
        5,
        "topic"::binary,
        # partitions array length
        0,
        0,
        0,
        1,
        # partition_index
        0,
        0,
        0,
        0,
        # error_code (0 = no error)
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [topic] = response.topics
      assert topic.name == "topic"
      [partition] = topic.partitions
      assert partition.partition_index == 0
      assert partition.error_code == 0
    end

    test "deserializes response with error" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # topics
        0,
        0,
        0,
        1,
        0,
        5,
        "topic"::binary,
        # partitions
        0,
        0,
        0,
        1,
        # partition_index
        0,
        0,
        0,
        0,
        # error_code (25 = UNKNOWN_MEMBER_ID)
        0,
        25
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 25
    end
  end

  describe "V1" do
    alias Kayrock.OffsetCommit.V1.Request

    test "serializes request with generation_id and member_id" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "my-group",
        generation_id: 5,
        member_id: "member-123",
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 8
      assert api_version == 1
    end
  end

  describe "V2" do
    alias Kayrock.OffsetCommit.V2.Request

    test "serializes request with retention_time" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        group_id: "my-group",
        generation_id: 5,
        member_id: "member-123",
        # 24 hours
        retention_time_ms: 86_400_000,
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 8
      assert api_version == 2
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:offset_commit) do
        module = Module.concat([Kayrock, OffsetCommit, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          topics: []
        }

        fields =
          cond do
            version >= 2 ->
              Map.merge(base, %{
                generation_id: 1,
                member_id: "member",
                retention_time_ms: -1
              })

            version >= 1 ->
              Map.merge(base, %{generation_id: 1, member_id: "member"})

            true ->
              base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 8
        assert api_version == version
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "edge cases" do
    alias Kayrock.OffsetCommit.V0.Request
    alias Kayrock.OffsetCommit.V0.Response

    test "multiple topics and partitions serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic-1",
            partitions: [
              %{partition_index: 0, committed_offset: 100, committed_metadata: ""},
              %{partition_index: 1, committed_offset: 200, committed_metadata: ""},
              %{partition_index: 2, committed_offset: 300, committed_metadata: ""}
            ]
          },
          %{
            name: "topic-2",
            partitions: [
              %{partition_index: 0, committed_offset: 50, committed_metadata: ""}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "topic-1")
      assert String.contains?(serialized, "topic-2")
    end

    test "high offset value serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 9_223_372_036_854_775_807,
                committed_metadata: ""
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "with committed_metadata serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_metadata: "consumer-instance-1:timestamp-12345"
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "consumer-instance-1")
    end

    test "response with multiple topic errors deserializes correctly" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # 2 topics
        0,
        0,
        0,
        2,
        0,
        7,
        "topic-1"::binary,
        # 1 partition
        0,
        0,
        0,
        1,
        # partition 0
        0,
        0,
        0,
        0,
        # no error
        0,
        0,
        0,
        7,
        "topic-2"::binary,
        # 1 partition
        0,
        0,
        0,
        1,
        # partition 0
        0,
        0,
        0,
        0,
        # UNKNOWN_TOPIC_OR_PARTITION
        0,
        3
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert length(response.topics) == 2
      [topic1, topic2] = response.topics
      [p1] = topic1.partitions
      [p2] = topic2.partitions
      assert p1.error_code == 0
      assert p2.error_code == 3
    end

    test "response with OFFSET_METADATA_TOO_LARGE error deserializes" do
      response_binary = <<
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        5,
        "topic"::binary,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        # OFFSET_METADATA_TOO_LARGE
        0,
        12
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 12
    end
  end
end
