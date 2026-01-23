defmodule Kayrock.Apis.OffsetFetchTest do
  @moduledoc """
  Tests for OffsetFetch API (V0-V8).

  API Key: 9
  Used to: Fetch committed offsets for a consumer group.

  Protocol structure:
  - V0: Basic offset fetch by topic/partition
  - V2+: Adds group-level error_code in response
  - V5+: Compact format
  - V8+: Adds require_stable flag
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.OffsetFetch.V0.Request
    alias Kayrock.OffsetFetch.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "my-group",
        topics: [
          %{
            name: "topic",
            partition_indexes: [0, 1, 2]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 9
      assert api_version == 0
    end

    test "serializes request with multiple topics" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        topics: [
          %{name: "topic-1", partition_indexes: [0]},
          %{name: "topic-2", partition_indexes: [0, 1]}
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
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
        # committed_offset (100)
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100,
        # metadata (null)
        255,
        255,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [topic] = response.topics
      assert topic.name == "topic"
      [partition] = topic.partitions
      assert partition.partition_index == 0
      assert partition.committed_offset == 100
      assert partition.error_code == 0
    end

    test "deserializes response with metadata" do
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
        # committed_offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        50,
        # metadata
        0,
        11,
        "my-metadata"::binary,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.committed_offset == 50
      assert partition.metadata == "my-metadata"
    end

    test "deserializes response with error" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
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
        # offset (-1 = no offset)
        255,
        255,
        255,
        255,
        255,
        255,
        255,
        255,
        # metadata (null)
        255,
        255,
        # error_code (3 = UNKNOWN_TOPIC_OR_PARTITION)
        0,
        3
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.committed_offset == -1
      assert partition.error_code == 3
    end
  end

  describe "V1" do
    alias Kayrock.OffsetFetch.V1.Request

    test "serializes request (same as V0)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        topics: [%{name: "topic", partition_indexes: [0]}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 9
      assert api_version == 1
    end
  end

  describe "V2" do
    alias Kayrock.OffsetFetch.V2.Request
    alias Kayrock.OffsetFetch.V2.Response

    test "serializes request (same as V0/V1)" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        group_id: "group",
        topics: [%{name: "topic", partition_indexes: [0]}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 9
      assert api_version == 2
    end

    test "deserializes response with group error_code" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # topics (empty)
        0,
        0,
        0,
        0,
        # error_code (V2+, group-level)
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 2
      assert response.error_code == 0
    end

    test "deserializes response with group error" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        3,
        # topics (empty)
        0,
        0,
        0,
        0,
        # error_code (16 = NOT_COORDINATOR)
        0,
        16
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 16
    end
  end

  describe "V3" do
    alias Kayrock.OffsetFetch.V3.Response

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        3,
        # throttle_time_ms (100)
        0,
        0,
        0,
        100,
        # topics (empty)
        0,
        0,
        0,
        0,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == 100
      assert response.error_code == 0
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:offset_fetch) do
        module = Module.concat([Kayrock, OffsetFetch, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          group_id: "group",
          topics: [%{name: "topic", partition_indexes: [0]}]
        }

        fields =
          if version >= 8 do
            Map.put(base, :require_stable, false)
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 9
        assert api_version == version
      end
    end
  end
end
