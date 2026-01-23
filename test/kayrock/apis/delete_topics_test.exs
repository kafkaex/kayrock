defmodule Kayrock.Apis.DeleteTopicsTest do
  @moduledoc """
  Tests for DeleteTopics API (V0-V4).

  API Key: 20
  Used to: Delete topics from the Kafka cluster.

  Protocol structure:
  - V0: Basic topic deletion
  - V1+: Adds throttle_time in response
  - V4+: Compact format
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  describe "V0" do
    alias Kayrock.DeleteTopics.V0.Request
    alias Kayrock.DeleteTopics.V0.Response

    test "serializes request" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        topic_names: ["topic1", "topic2"],
        timeout_ms: 30000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized

      assert api_key == 20
      assert api_version == 0
    end

    test "serializes request with single topic" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topic_names: ["single-topic"],
        timeout_ms: 30000
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
        2,
        # topic name
        0,
        6,
        "topic1"::binary,
        # error_code
        0,
        0,
        # topic name
        0,
        6,
        "topic2"::binary,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert length(response.responses) == 2

      [topic1, topic2] = response.responses
      assert topic1.name == "topic1"
      assert topic1.error_code == 0
      assert topic2.name == "topic2"
    end

    test "deserializes error response" do
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
        # error_code (3 = UNKNOWN_TOPIC_OR_PARTITION)
        0,
        3
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.responses
      assert topic.error_code == 3
    end
  end

  describe "V1" do
    alias Kayrock.DeleteTopics.V1.Response

    test "deserializes response with throttle_time" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # throttle_time_ms
        0,
        0,
        0,
        50,
        # responses
        0,
        0,
        0,
        1,
        0,
        5,
        "topic"::binary,
        # error_code
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.throttle_time_ms == 50
      [topic] = response.responses
      assert topic.error_code == 0
    end
  end

  describe "version compatibility" do
    test "all available versions serialize" do
      for version <- api_version_range(:delete_topics) do
        module = Module.concat([Kayrock, DeleteTopics, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        request =
          struct(module, %{
            correlation_id: version,
            client_id: "test",
            topic_names: [],
            timeout_ms: 30000
          })

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 20
        assert api_version == version
      end
    end
  end
end
