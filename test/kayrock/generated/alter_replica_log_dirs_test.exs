defmodule Kayrock.AlterReplicaLogDirsTest do
  @moduledoc """
  Tests for AlterReplicaLogDirs API (V0-V1).

  API Key: 34
  Used to: Move replicas to different log directories.

  Protocol structure:
  - V0-V1: Same schema - log_dirs array with topics and partitions
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.AlterReplicaLogDirsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = AlterReplicaLogDirsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 34
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = AlterReplicaLogDirsFactory.response_data(version)

        response_module = Module.concat([Kayrock.AlterReplicaLogDirs, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.topics)
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, AlterReplicaLogDirs, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, AlterReplicaLogDirs, :"V#{version}", Response])

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

  describe "V0 - basic alter replica log dirs" do
    alias Kayrock.AlterReplicaLogDirs.V0.Request
    alias Kayrock.AlterReplicaLogDirs.V0.Response

    test "request serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        log_dirs: [
          %{
            log_dir: "/var/kafka/logs",
            topics: [
              %{topic: "my-topic", partitions: [0, 1, 2]}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 34
      assert api_version == 0
    end

    test "response deserializes correctly" do
      {response_binary, expected} = AlterReplicaLogDirsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0

      [topic] = response.topics
      assert topic.topic == "topic1"
      [partition] = topic.partitions
      assert partition.partition == 0
      assert partition.error_code == 0
    end
  end

  describe "V1 - same schema as V0" do
    alias Kayrock.AlterReplicaLogDirs.V1.Response

    test "response with throttle_time_ms deserializes correctly" do
      {response_binary, expected} = AlterReplicaLogDirsFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, AlterReplicaLogDirs, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            log_dirs: []
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, AlterReplicaLogDirs, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            log_dirs: []
          )

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..1 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = AlterReplicaLogDirsFactory.response_data(version)
        response_module = Module.concat([Kayrock.AlterReplicaLogDirs, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end
  end

  describe "extra bytes handling" do
    for version <- 0..1 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = AlterReplicaLogDirsFactory.response_data(version)
        response_module = Module.concat([Kayrock.AlterReplicaLogDirs, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  describe "malformed response handling" do
    for version <- 0..1 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.AlterReplicaLogDirs, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  describe "error code handling" do
    @error_codes [
      {0, 58, "NOT_CONTROLLER"},
      {1, 34, "LOG_DIR_NOT_FOUND"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.AlterReplicaLogDirs, :"V#{version}", Response])

        response_binary =
          AlterReplicaLogDirsFactory.error_response(version, error_code: error_code)

        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.topics
        [partition] = topic.partitions
        assert partition.error_code == error_code
      end
    end
  end
end
