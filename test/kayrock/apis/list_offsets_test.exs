defmodule Kayrock.Apis.ListOffsetsTest do
  @moduledoc """
  Tests for ListOffsets API (V0-V5).

  API Key: 2
  Used to: Get available offsets for topic partitions.

  Protocol structure:
  - V0: Basic offset fetch with max_num_offsets
  - V1+: Removes max_num_offsets, returns single offset
  - V2+: Adds isolation_level
  - V4+: Adds current_leader_epoch
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.ListOffsets.V0.Request
    alias Kayrock.ListOffsets.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{
                partition: 0,
                timestamp: -1,
                max_num_offsets: 1
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 0
    end

    test "serializes request for latest offset" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              # -1 = latest
              %{partition: 0, timestamp: -1, max_num_offsets: 1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "serializes request for earliest offset" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              # -2 = earliest
              %{partition: 0, timestamp: -2, max_num_offsets: 1}
            ]
          }
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
        # responses array length
        0,
        0,
        0,
        1,
        # topic
        0,
        5,
        "topic"::binary,
        # partition_responses
        0,
        0,
        0,
        1,
        # partition
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # offsets array (V0 returns array)
        0,
        0,
        0,
        1,
        # offset (100)
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [topic] = response.responses
      assert topic.topic == "topic"
      [partition] = topic.partition_responses
      assert partition.partition == 0
      assert partition.error_code == 0
    end
  end

  describe "V1" do
    alias Kayrock.ListOffsets.V1.Request
    alias Kayrock.ListOffsets.V1.Response

    test "serializes request (no max_num_offsets)" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        replica_id: -1,
        topics: [
          %{
            topic: "topic",
            partitions: [
              %{partition: 0, timestamp: -1}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 1
    end

    test "deserializes response with single offset" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # responses
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
        # partition
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # timestamp
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        42,
        # offset (single, not array)
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.responses
      [partition] = topic.partition_responses
      assert partition.offset == 100
    end
  end

  describe "V2" do
    alias Kayrock.ListOffsets.V2.Request

    test "serializes request with isolation_level" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        replica_id: -1,
        # 0 = read_uncommitted, 1 = read_committed
        isolation_level: 0,
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 2
      assert api_version == 2
    end

    test "serializes request with read_committed isolation" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        replica_id: -1,
        # read_committed
        isolation_level: 1,
        topics: []
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:list_offsets) do
        module = Module.concat([Kayrock, ListOffsets, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          replica_id: -1,
          topics: []
        }

        fields =
          if version >= 2 do
            Map.put(base, :isolation_level, 0)
          else
            base
          end

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 2
        assert api_version == version
      end
    end
  end
end
