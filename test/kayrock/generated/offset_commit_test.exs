defmodule Kayrock.OffsetCommitTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.OffsetCommitFactory

  # ============================================
  # Version Compatibility Tests (Factory-Based)
  # ============================================

  describe "versions compatibility" do
    test "all available versions serialize correctly" do
      for version <- api_version_range(:offset_commit) do
        {request, expected_binary} = OffsetCommitFactory.request_data(version)

        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request should match expected binary"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 8, "V#{version} should have API key 8"
        assert api_version == version, "V#{version} should have correct version"
      end
    end

    test "all available versions deserialize correctly" do
      for version <- api_version_range(:offset_commit) do
        {response_binary, expected_struct} = OffsetCommitFactory.response_data(version)

        response_module = Module.concat([Kayrock, OffsetCommit, :"V#{version}", Response])
        {actual, <<>>} = response_module.deserialize(response_binary)

        assert actual == expected_struct,
               "V#{version} response should match expected struct"
      end
    end
  end

  # ============================================
  # Version-Specific Tests
  # ============================================

  describe "V0" do
    alias Kayrock.OffsetCommit.V0.Request
    alias Kayrock.OffsetCommit.V0.Response

    test "serializes request with topics and partitions" do
      {request, expected_binary} = OffsetCommitFactory.request_data(0)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary
    end

    test "deserializes response with success" do
      {response_binary, expected_struct} = OffsetCommitFactory.response_data(0)

      {response, <<>>} = Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.correlation_id == 0
      [topic] = response.topics
      assert topic.name == "topic"
      [partition] = topic.partitions
      assert partition.partition_index == 0
      assert partition.error_code == 0
    end

    test "deserializes response with error" do
      response_binary = OffsetCommitFactory.error_response(0, error_code: 25, correlation_id: 1)

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 25
    end
  end

  describe "V1" do
    test "serializes request with generation_id and member_id" do
      {request, expected_binary} = OffsetCommitFactory.request_data(1)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 8
      assert api_version == 1
    end
  end

  describe "V2" do
    test "serializes request with retention_time" do
      {request, expected_binary} = OffsetCommitFactory.request_data(2)

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 8
      assert api_version == 2
    end
  end

  describe "V3" do
    test "response includes throttle_time_ms" do
      {response_binary, expected_struct} = OffsetCommitFactory.response_data(3)

      {response, <<>>} = Kayrock.OffsetCommit.V3.Response.deserialize(response_binary)

      assert response == expected_struct
      assert response.throttle_time_ms == 100
    end
  end

  # ============================================
  # Edge Cases
  # ============================================

  describe "edge cases" do
    alias Kayrock.OffsetCommit.V0.Request
    alias Kayrock.OffsetCommit.V0.Response

    test "multiple topics and partitions serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic-1",
            partitions: [
              %{partition_index: 0, committed_offset: 100, committed_metadata: ""},
              %{partition_index: 1, committed_offset: 200, committed_metadata: ""},
              %{partition_index: 2, committed_offset: 300, committed_metadata: ""}
            ]
          },
          %{
            name: "topic-2",
            partitions: [
              %{partition_index: 0, committed_offset: 50, committed_metadata: ""}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "topic-1")
      assert String.contains?(serialized, "topic-2")
    end

    test "high offset value serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 9_223_372_036_854_775_807,
                committed_metadata: ""
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "with committed_metadata serializes correctly" do
      request = %Request{
        correlation_id: 0,
        client_id: "test",
        group_id: "group",
        topics: [
          %{
            name: "topic",
            partitions: [
              %{
                partition_index: 0,
                committed_offset: 100,
                committed_metadata: "consumer-instance-1:timestamp-12345"
              }
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "consumer-instance-1")
    end

    test "response with multiple topic errors deserializes correctly" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # 2 topics
        0,
        0,
        0,
        2,
        0,
        7,
        "topic-1"::binary,
        # 1 partition
        0,
        0,
        0,
        1,
        # partition 0
        0,
        0,
        0,
        0,
        # no error
        0,
        0,
        0,
        7,
        "topic-2"::binary,
        # 1 partition
        0,
        0,
        0,
        1,
        # partition 0
        0,
        0,
        0,
        0,
        # UNKNOWN_TOPIC_OR_PARTITION
        0,
        3
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert length(response.topics) == 2
      [topic1, topic2] = response.topics
      [p1] = topic1.partitions
      [p2] = topic2.partitions
      assert p1.error_code == 0
      assert p2.error_code == 3
    end

    test "response with OFFSET_METADATA_TOO_LARGE error deserializes" do
      response_binary = <<
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        1,
        0,
        5,
        "topic"::binary,
        0,
        0,
        0,
        1,
        0,
        0,
        0,
        0,
        # OFFSET_METADATA_TOO_LARGE
        0,
        12
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.error_code == 12
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "all versions handle truncated response binaries" do
      for version <- api_version_range(:offset_commit) do
        {response_binary, _expected_struct} = OffsetCommitFactory.response_data(version)
        response_module = Module.concat([Kayrock, OffsetCommit, :"V#{version}", Response])

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(
            response_module,
            response_binary,
            truncate_at
          )
        end
      end
    end
  end

  describe "extra bytes handling" do
    test "all versions handle extra trailing bytes correctly" do
      for version <- api_version_range(:offset_commit) do
        {response_binary, _expected_struct} = OffsetCommitFactory.response_data(version)
        response_module = Module.concat([Kayrock, OffsetCommit, :"V#{version}", Response])

        assert_extra_bytes_returned(
          response_module,
          response_binary,
          <<42, 43, 44>>
        )
      end
    end
  end

  describe "malformed response handling" do
    test "empty binary fails with MatchError" do
      assert_raise MatchError, fn ->
        Kayrock.OffsetCommit.V0.Response.deserialize(<<>>)
      end
    end

    test "invalid array length fails with FunctionClauseError" do
      invalid = <<
        0,
        0,
        0,
        0,
        # Claims 500 topics
        0,
        0,
        1,
        244
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.OffsetCommit.V0.Response.deserialize(invalid)
      end
    end
  end
end
