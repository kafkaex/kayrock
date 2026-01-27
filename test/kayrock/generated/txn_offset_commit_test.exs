defmodule Kayrock.TxnOffsetCommitTest do
  @moduledoc """
  Tests for TxnOffsetCommit API (V0-V2).

  API Key: 28
  Used to: Commit offsets within a transaction for a consumer group.

  Protocol structure:
  - V0-V2: Topics with partitions containing offset and metadata
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.TxnOffsetCommitFactory

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "versions compatibility" do
    for version <- 0..2 do
      test "serializes version #{version} request to expected binary" do
        version = unquote(version)
        {request, expected_binary} = TxnOffsetCommitFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert serialized == expected_binary

        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 28
        assert api_version == version
        assert correlation_id == request.correlation_id
      end

      test "deserializes version #{version} response to expected struct" do
        version = unquote(version)
        {response_binary, expected_struct} = TxnOffsetCommitFactory.response_data(version)

        response_module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])
        {actual_struct, rest} = response_module.deserialize(response_binary)

        assert rest == <<>>
        assert actual_struct == expected_struct
        assert actual_struct.correlation_id == version
        assert is_list(actual_struct.topics)
      end
    end

    test "all available versions have modules" do
      for version <- 0..2 do
        request_module = Module.concat([Kayrock, TxnOffsetCommit, :"V#{version}", Request])
        response_module = Module.concat([Kayrock, TxnOffsetCommit, :"V#{version}", Response])

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

  describe "V0 - basic txn offset commit" do
    alias Kayrock.TxnOffsetCommit.V0.Request
    alias Kayrock.TxnOffsetCommit.V0.Response

    test "request with metadata serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        transactional_id: "my-txn",
        group_id: "my-group",
        producer_id: 100,
        producer_epoch: 5,
        topics: [
          %{
            name: "topic1",
            partitions: [
              %{partition_index: 0, committed_offset: 100, committed_metadata: "meta1"}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)

      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 28
      assert api_version == 0
    end

    test "response with partition results deserializes correctly" do
      {response_binary, expected} = TxnOffsetCommitFactory.response_data(0)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 0
      assert length(response.topics) == 1

      [topic] = response.topics
      assert topic.name == "topic1"
      assert length(topic.partitions) == 1
    end
  end

  describe "V1 - same schema as V0" do
    alias Kayrock.TxnOffsetCommit.V1.Response

    test "response with throttle_time_ms deserializes correctly" do
      {response_binary, expected} = TxnOffsetCommitFactory.response_data(1)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 50
    end
  end

  describe "V2 - same schema" do
    alias Kayrock.TxnOffsetCommit.V2.Response

    test "response with multiple partitions deserializes correctly" do
      {response_binary, expected} = TxnOffsetCommitFactory.response_data(2)
      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected
      assert response.throttle_time_ms == 100

      [topic] = response.topics
      assert length(topic.partitions) == 2
    end
  end

  # ============================================
  # Integration with Request Protocol
  # ============================================

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, TxnOffsetCommit, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: "txn",
            group_id: "group",
            producer_id: 1,
            producer_epoch: 0,
            topics: []
          )

        assert Kayrock.Request.api_vsn(request) == version
      end
    end

    test "response_deserializer returns deserialize function" do
      for version <- 0..2 do
        module = Module.concat([Kayrock, TxnOffsetCommit, :"V#{version}", Request])

        request =
          struct(module,
            correlation_id: 0,
            client_id: "test",
            transactional_id: "txn",
            group_id: "group",
            producer_id: 1,
            producer_epoch: 0,
            topics: []
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
    for version <- 0..2 do
      test "V#{version} response handles truncated binary" do
        version = unquote(version)
        {response_binary, _} = TxnOffsetCommitFactory.response_data(version)
        response_module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])

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
    for version <- 0..2 do
      test "V#{version} response handles extra trailing bytes" do
        version = unquote(version)
        {response_binary, _} = TxnOffsetCommitFactory.response_data(version)
        response_module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])

        assert_extra_bytes_returned(response_module, response_binary, <<99, 88, 77>>)
      end
    end
  end

  # ============================================
  # Edge Cases - Malformed Response
  # ============================================

  describe "malformed response handling" do
    for version <- 0..2 do
      test "V#{version} empty binary fails with MatchError" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])

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
      {0, 51, "CONCURRENT_TRANSACTIONS"},
      {1, 47, "INVALID_PRODUCER_EPOCH"},
      {2, 22, "ILLEGAL_GENERATION"}
    ]

    for {version, error_code, name} <- @error_codes do
      test "V#{version} handles #{name} (#{error_code})" do
        version = unquote(version)
        error_code = unquote(error_code)
        response_module = Module.concat([Kayrock.TxnOffsetCommit, :"V#{version}", Response])

        response_binary = TxnOffsetCommitFactory.error_response(version, error_code: error_code)
        {response, <<>>} = response_module.deserialize(response_binary)

        [topic] = response.topics
        [partition] = topic.partitions
        assert partition.error_code == error_code
      end
    end
  end
end
