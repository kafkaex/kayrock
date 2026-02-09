defmodule Kayrock.OffsetDeleteTest do
  @moduledoc """
  Tests for OffsetDelete API (V0).

  API Key: 47
  Used to: Delete committed offsets for a consumer group.

  Protocol structure:
  - V0: group_id with topics containing partition indices
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.OffsetDelete.V0.{Request, Response}
  alias Kayrock.Test.Factories.OffsetDeleteFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    test "serializes version 0 request to expected binary" do
      {request, expected_binary} = OffsetDeleteFactory.request_data(0)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary

      <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
      assert api_key == 47
      assert api_version == 0
      assert correlation_id == request.correlation_id
    end

    test "deserializes version 0 response to expected struct" do
      {response_binary, expected_struct} = OffsetDeleteFactory.response_data(0)

      {actual_struct, rest} = Response.deserialize(response_binary)

      assert rest == <<>>
      assert actual_struct == expected_struct
      assert actual_struct.correlation_id == 0
      assert is_list(actual_struct.topics)
    end

    test "all available versions have modules" do
      request_module = Request
      response_module = Response

      assert Code.ensure_loaded?(request_module),
             "Request module #{inspect(request_module)} should exist"

      assert Code.ensure_loaded?(response_module),
             "Response module #{inspect(response_module)} should exist"
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic offset delete" do
    alias Request
    alias Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "my-group",
        topics: [
          %{
            name: "topic1",
            partitions: [%{partition_index: 0}, %{partition_index: 1}]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 47
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = OffsetDeleteFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.error_code == 0
      assert response.throttle_time_ms == 0

      [topic] = response.topics
      assert topic.name == "topic1"
      [partition] = topic.partitions
      assert partition.partition_index == 0
      assert partition.error_code == 0
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: []
      }

      assert Kayrock.Request.api_vsn(request) == 0
    end

    test "response_deserializer returns deserialize function" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: []
      }

      deserializer = Kayrock.Request.response_deserializer(request)
      assert is_function(deserializer, 1)
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "V0 response handles truncated binary" do
      {response_binary, _} = OffsetDeleteFactory.response_data(0)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(Response, response_binary, truncate_at)
      end
    end
  end

  describe "extra bytes handling" do
    test "V0 response handles extra trailing bytes" do
      {response_binary, _} = OffsetDeleteFactory.response_data(0)

      assert_extra_bytes_returned(
        Response,
        response_binary,
        <<99, 88, 77>>
      )
    end
  end

  describe "malformed response handling" do
    test "V0 empty binary fails with MatchError" do
      assert_raise MatchError, fn ->
        Response.deserialize(<<>>)
      end
    end
  end

  describe "error code handling" do
    test "V0 handles GROUP_ID_NOT_FOUND (69)" do
      response_binary = OffsetDeleteFactory.error_response(0, error_code: 69)
      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 69
    end

    test "V0 handles GROUP_SUBSCRIBED_TO_TOPIC (86)" do
      response_binary = OffsetDeleteFactory.error_response(0, error_code: 86)
      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 86
    end
  end
end
