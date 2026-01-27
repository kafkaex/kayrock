defmodule Kayrock.DeleteTopicsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DeleteTopicsFactory

  @versions 0..4

  # ============================================
  # Version Compatibility Tests (Gold Standard)
  # ============================================

  describe "versions compatibility" do
    for version <- 0..4 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = DeleteTopicsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        # Verify header fields
        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 20
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = DeleteTopicsFactory.response_data(version)

        {actual_struct, rest} =
          Module.concat([Kayrock.DeleteTopics, :"V#{version}", Response]).deserialize(
            response_binary
          )

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.responses)
      end
    end

    test "all available versions have modules" do
      for version <- @versions do
        request_module = Module.concat([Kayrock, DeleteTopics, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, DeleteTopics, :"V#{version}", Response])

        assert Code.ensure_loaded?(request_module),
               "Request module #{inspect(request_module)} should exist"

        assert Code.ensure_loaded?(response_module),
               "Response module #{inspect(response_module)} should exist"
      end
    end
  end

  # ============================================
  # Version-Specific Features
  # ============================================

  describe "V0 - basic topic deletion" do
    alias Kayrock.DeleteTopics.V0.Request

    test "serializes request with multiple topics" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topic_names: ["topic1", "topic2", "topic3"],
        timeout_ms: 60_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 20
      assert api_version == 0
    end

    test "deserializes multi-topic response" do
      response_binary = DeleteTopicsFactory.multi_topic_response(0)
      {response, <<>>} = Kayrock.DeleteTopics.V0.Response.deserialize(response_binary)

      assert length(response.responses) == 2
      [success, error] = response.responses
      assert success.name == "topic-success"
      assert success.error_code == 0
      assert error.name == "topic-error"
      assert error.error_code == 3
    end
  end

  describe "V1+ - throttle_time_ms" do
    test "deserializes response with throttle_time" do
      {response_binary, expected_struct} = DeleteTopicsFactory.response_data(1)
      {response, <<>>} = Kayrock.DeleteTopics.V1.Response.deserialize(response_binary)

      assert response.throttle_time_ms == expected_struct.throttle_time_ms
      assert response.throttle_time_ms == 50
    end

    test "deserializes response with different throttle times" do
      response_binary = DeleteTopicsFactory.response_binary(2, throttle_time_ms: 500)
      {response, <<>>} = Kayrock.DeleteTopics.V2.Response.deserialize(response_binary)

      assert response.throttle_time_ms == 500
    end
  end

  describe "V4 - flexible/compact format" do
    alias Kayrock.DeleteTopics.V4.Response

    test "deserializes response with tagged_fields" do
      {response_binary, expected_struct} = DeleteTopicsFactory.response_data(4)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.tagged_fields == []

      [topic] = response.responses
      assert topic.name == "topic"
      assert topic.error_code == 0
      assert topic.tagged_fields == []
    end

    test "deserializes multi-topic response with V4 fields" do
      response_binary = DeleteTopicsFactory.multi_topic_response(4)
      {response, <<>>} = Response.deserialize(response_binary)

      assert length(response.responses) == 2
      [success, error] = response.responses

      assert success.name == "topic-success"
      assert success.error_code == 0
      assert error.name == "topic-error"
      assert error.error_code == 3
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- @versions do
        module = Module.concat([Kayrock, DeleteTopics, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test")

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- @versions do
        module = Module.concat([Kayrock, DeleteTopics, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test")

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases - Truncated Binary
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..4 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = DeleteTopicsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteTopics, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  # ============================================
  # Edge Cases - Extra Bytes
  # ============================================

  describe "extra bytes handling" do
    for version <- 0..4 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = DeleteTopicsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteTopics, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..4 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.DeleteTopics, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 invalid responses array length fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # Claims 500 topics (but no data follows)
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DeleteTopics.V0.Response.deserialize(invalid)
      end
    end

    test "V1 invalid responses array length fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # Claims 500 topics (but no data follows)
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DeleteTopics.V1.Response.deserialize(invalid)
      end
    end

    test "V4 invalid compact array fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # header tagged_fields
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # responses compact array with huge value (254 topics, but no data)
        255
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.DeleteTopics.V4.Response.deserialize(invalid)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 3, "UNKNOWN_TOPIC_OR_PARTITION"},
      {1, 29, "TOPIC_AUTHORIZATION_FAILED"},
      {2, 41, "NOT_CONTROLLER"},
      {3, 73, "TOPIC_DELETION_DISABLED"},
      {4, 36, "TOPIC_ALREADY_EXISTS"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.DeleteTopics, :"V#{version}", Response])

        response_binary = DeleteTopicsFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.responses
        assert topic.error_code == error_code
      end
    end

    test "handles negative error codes (UNKNOWN_SERVER_ERROR)" do
      response_binary = DeleteTopicsFactory.error_response(0, error_code: -1)
      {response, <<>>} = Kayrock.DeleteTopics.V0.Response.deserialize(response_binary)

      [topic] = response.responses
      assert topic.error_code == -1
    end
  end
end
