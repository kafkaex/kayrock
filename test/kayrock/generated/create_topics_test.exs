defmodule Kayrock.CreateTopicsTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.CreateTopicsFactory

  @versions api_version_range(:create_topics)

  # ============================================
  # Version Compatibility Tests (Gold Standard)
  # ============================================

  describe "versions compatibility" do
    for version <- 0..5 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = CreateTopicsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        # Verify header fields
        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 19
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = CreateTopicsFactory.response_data(version)

        {actual_struct, rest} =
          Module.concat([Kayrock.CreateTopics, :"V#{version}", Response]).deserialize(
            response_binary
          )

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.topics)
      end
    end

    test "all available versions have modules" do
      for version <- @versions do
        request_module = Module.concat([Kayrock, CreateTopics, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, CreateTopics, :"V#{version}", Response])

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

  describe "V0 - basic topic creation" do
    alias Kayrock.CreateTopics.V0.Request

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

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 19
      assert api_version == 0
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

    test "serializes request with manual partition assignments" do
      request = %Request{
        correlation_id: 3,
        client_id: "test",
        topics: [
          %{
            name: "topic",
            num_partitions: -1,
            replication_factor: -1,
            assignments: [
              %{partition_index: 0, broker_ids: [1, 2]},
              %{partition_index: 1, broker_ids: [2, 3]}
            ],
            configs: []
          }
        ],
        timeout_ms: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "deserializes multi-topic response" do
      response_binary = CreateTopicsFactory.multi_topic_response(0)
      {response, <<>>} = Kayrock.CreateTopics.V0.Response.deserialize(response_binary)

      assert length(response.topics) == 2
      [topic1, topic2] = response.topics
      assert topic1.name == "topic-1"
      assert topic1.error_code == 0
      assert topic2.name == "topic-2"
      assert topic2.error_code == 36
    end
  end

  describe "V1 - validate_only flag" do
    alias Kayrock.CreateTopics.V1.Request
    alias Kayrock.CreateTopics.V1.Response

    test "serializes request with validate_only true" do
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
      response_binary =
        CreateTopicsFactory.error_response(1,
          error_code: 36,
          error_message: "Topic already exists"
        )

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == 36
      assert topic.error_message == "Topic already exists"
    end

    test "deserializes response with null error_message" do
      response_binary = CreateTopicsFactory.response_binary(1)
      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == 0
      assert topic.error_message == nil
    end
  end

  describe "V2+ - throttle_time_ms" do
    test "deserializes response with throttle_time" do
      {response_binary, expected_struct} = CreateTopicsFactory.response_data(2)
      {response, <<>>} = Kayrock.CreateTopics.V2.Response.deserialize(response_binary)

      assert response.throttle_time_ms == expected_struct.throttle_time_ms
      assert response.throttle_time_ms == 100
    end

    test "deserializes response with different throttle times" do
      response_binary = CreateTopicsFactory.response_binary(2, throttle_time_ms: 500)
      {response, <<>>} = Kayrock.CreateTopics.V2.Response.deserialize(response_binary)

      assert response.throttle_time_ms == 500
    end
  end

  describe "V5 - flexible/compact format" do
    alias Kayrock.CreateTopics.V5.Response

    test "deserializes response with additional topic metadata" do
      response_binary =
        CreateTopicsFactory.response_binary(5,
          topics: [
            %{
              name: "my-topic",
              error_code: 0,
              error_message: nil,
              num_partitions: 10,
              replication_factor: 3
            }
          ]
        )

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.name == "my-topic"
      assert topic.num_partitions == 10
      assert topic.replication_factor == 3
      assert topic.configs == []
      assert topic.tagged_fields == []
    end

    test "deserializes multi-topic response with V5 fields" do
      response_binary = CreateTopicsFactory.multi_topic_response(5)
      {response, <<>>} = Response.deserialize(response_binary)

      assert length(response.topics) == 2
      [topic1, topic2] = response.topics

      assert topic1.name == "topic-1"
      assert topic1.error_code == 0
      assert topic1.num_partitions == 3

      assert topic2.name == "topic-2"
      assert topic2.error_code == 36
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- @versions do
        module = Module.concat([Kayrock, CreateTopics, :"V#{version}", Request])
        request = struct(module, correlation_id: 0, client_id: "test")

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- @versions do
        module = Module.concat([Kayrock, CreateTopics, :"V#{version}", Request])
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
    for version <- 0..5 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = CreateTopicsFactory.response_data(version)
        response_module = Module.concat([Kayrock.CreateTopics, :"V#{version}", Response])

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
    for version <- 0..5 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = CreateTopicsFactory.response_data(version)
        response_module = Module.concat([Kayrock.CreateTopics, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..5 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.CreateTopics, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end

    test "V0 invalid topics array length fails with FunctionClauseError" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # Claims 800 topics (but no data follows)
        0,
        0,
        3,
        32
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.CreateTopics.V0.Response.deserialize(invalid)
      end
    end

    test "V2 invalid topics array length fails with FunctionClauseError" do
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
        # Claims 1000 topics (but no data follows)
        0,
        0,
        3,
        232
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.CreateTopics.V2.Response.deserialize(invalid)
      end
    end

    test "V5 invalid compact array fails with FunctionClauseError" do
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
        # topics compact array with huge value (254 topics, but no data)
        255
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.CreateTopics.V5.Response.deserialize(invalid)
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 36, "TOPIC_ALREADY_EXISTS"},
      {1, 37, "INVALID_PARTITIONS"},
      {2, 38, "INVALID_REPLICATION_FACTOR"},
      {3, 40, "INVALID_CONFIG"},
      {4, 29, "TOPIC_AUTHORIZATION_FAILED"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.CreateTopics, :"V#{version}", Response])

        response_binary = CreateTopicsFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.topics
        assert topic.error_code == error_code
      end
    end

    test "handles negative error codes (UNKNOWN_SERVER_ERROR)" do
      response_binary = CreateTopicsFactory.error_response(0, error_code: -1)
      {response, <<>>} = Kayrock.CreateTopics.V0.Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == -1
    end

    test "V5 error response includes metadata fields" do
      response_binary = CreateTopicsFactory.error_response(5, error_code: 36)
      {response, <<>>} = Kayrock.CreateTopics.V5.Response.deserialize(response_binary)

      [topic] = response.topics
      assert topic.error_code == 36
      assert topic.num_partitions == -1
      assert topic.replication_factor == -1
    end
  end
end
