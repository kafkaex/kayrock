defmodule Kayrock.Apis.CreateTopicsTest do
  @moduledoc """
  Tests for CreateTopics API (V0-V5).

  API Key: 19
  Used to: Create new topics on the Kafka cluster.

  Protocol structure:
  - V0: Basic topic creation
  - V1+: Adds validate_only flag
  - V5+: Compact format
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.CreateTopics.V0.Request
    alias Kayrock.CreateTopics.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        topics: [
          %{
            name: "new-topic",
            num_partitions: 3,
            replication_factor: 1,
            assignments: [],
            configs: []
          }
        ],
        timeout_ms: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 19
      assert api_version == 0
    end

    test "serializes request with multiple topics" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [
          %{
            name: "topic-1",
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: []
          },
          %{
            name: "topic-2",
            num_partitions: 3,
            replication_factor: 3,
            assignments: [],
            configs: []
          }
        ],
        timeout_ms: 60_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "serializes request with configs" do
      request = %Request{
        correlation_id: 2,
        client_id: "test",
        topics: [
          %{
            name: "topic",
            num_partitions: 1,
            replication_factor: 1,
            assignments: [],
            configs: [
              %{name: "retention.ms", value: "86400000"},
              %{name: "cleanup.policy", value: "compact"}
            ]
          }
        ],
        timeout_ms: 30_000
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
        9,
        "new-topic"::binary,
        # error_code (0 = no error)
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      [topic] = response.topics
      assert topic.name == "new-topic"
      assert topic.error_code == 0
    end

    test "deserializes error response" do
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
        # error_code (36 = TOPIC_ALREADY_EXISTS)
        0,
        36
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == 36
    end
  end

  describe "V1" do
    alias Kayrock.CreateTopics.V1.Request
    alias Kayrock.CreateTopics.V1.Response

    test "serializes request with validate_only" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [],
        timeout_ms: 30_000,
        validate_only: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 19
      assert api_version == 1
    end

    test "deserializes response with error_message" do
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
        # error_code
        0,
        36,
        # error_message (null)
        255,
        255
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == 36
    end
  end

  describe "V2" do
    alias Kayrock.CreateTopics.V2.Response

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # throttle_time_ms
        0,
        0,
        0,
        100,
        # topics
        0,
        0,
        0,
        1,
        0,
        5,
        "topic"::binary,
        # error_code
        0,
        0,
        # error_message
        255,
        255
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == 100
      [topic] = response.topics
      assert topic.error_code == 0
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:create_topics) do
        module = Module.concat([Kayrock, CreateTopics, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        base = %{
          correlation_id: version,
          client_id: "test",
          topics: [],
          timeout_ms: 30_000
        }

        fields = if version >= 1, do: Map.put(base, :validate_only, false), else: base

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 19
        assert api_version == version
      end
    end
  end
end
