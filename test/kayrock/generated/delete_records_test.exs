defmodule Kayrock.DeleteRecordsTest do
  @moduledoc """
  Tests for DeleteRecords API (V0-V1).

  API Key: 21
  Used to: Delete records from a topic partition up to a specified offset.

  Protocol structure:
  - V0-V1: Same schema - topics with partitions and offsets
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.DeleteRecordsFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..1 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = DeleteRecordsFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 21
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = DeleteRecordsFactory.response_data(version)

        response_module = Module.concat([Kayrock.DeleteRecords, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.topics)
      end
    end

    test "all available versions have modules" do
      for version <- 0..1 do
        request_module = Module.concat([Kayrock, DeleteRecords, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, DeleteRecords, :"V#{version}", Response])

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

  describe "V0 - basic delete records" do
    alias Kayrock.DeleteRecords.V0.Request
    alias Kayrock.DeleteRecords.V0.Response

    test "request with single partition serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [
          %{
            topic: "topic1",
            partitions: [%{partition: 0, offset: 500}]
          }
        ],
        timeout: 30_000
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 21
      assert api_version == 0
    end

    test "response with low_watermark deserializes correctly" do
      {response_binary, expected} = DeleteRecordsFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.low_watermark == 100
      assert partition.error_code == 0
    end
  end

  describe "V1 - same schema as V0" do
    alias Kayrock.DeleteRecords.V1.Response

    test "response with multiple partitions deserializes correctly" do
      {response_binary, expected} = DeleteRecordsFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50

      [topic] = response.topics
      assert length(topic.partitions) == 2
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, DeleteRecords, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            topics: [],
            timeout: 30_000
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..1 do
        module = Module.concat([Kayrock, DeleteRecords, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            topics: [],
            timeout: 30_000
          )

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1)
      end
    end
  end

  # ============================================
  # Edge Cases - Truncated Binary
  # ============================================

  describe "truncated binary handling" do
    for version <- 0..1 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = DeleteRecordsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteRecords, :"V#{version}", Response])

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
    for version <- 0..1 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = DeleteRecordsFactory.response_data(version)
        response_module = Module.concat([Kayrock.DeleteRecords, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..1 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.DeleteRecords, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          response_module.deserialize(<<>>)
        end
      end
    end
  end

  # ============================================
  # Error Code Handling
  # ============================================

  describe "error code handling" do
    @error_codes [
      {0, 3, "UNKNOWN_TOPIC_OR_PARTITION"},
      {1, 83, "POLICY_VIOLATION"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.DeleteRecords, :"V#{version}", Response])

        response_binary = DeleteRecordsFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.topics
        [partition] = topic.partitions
        assert partition.error_code == error_code
        assert partition.low_watermark == -1
      end
    end
  end
end
