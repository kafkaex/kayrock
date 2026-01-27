defmodule Kayrock.OffsetFetchTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.OffsetFetchFactory

  # ============================================
  # Request Serialization Tests
  # ============================================

  describe "request serialization" do
    test "V0 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(0)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V1 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(1)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V2 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(2)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V3 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(3)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V4 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(4)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V5 request serializes correctly" do
      {request, expected_binary} = OffsetFetchFactory.request_data(5)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "V6 request serializes correctly (flexible)" do
      {request, expected_binary} = OffsetFetchFactory.request_data(6)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected_binary
    end

    test "request with multiple topics serializes" do
      request = %Kayrock.OffsetFetch.V0.Request{
        correlation_id: 1,
        client_id: "test",
        group_id: "group",
        topics: [
          %{name: "topic-1", partition_indexes: [0]},
          %{name: "topic-2", partition_indexes: [0, 1]}
        ]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  # ============================================
  # Response Deserialization Tests
  # ============================================

  describe "response deserialization" do
    test "V0 response deserializes correctly" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(0)
      {actual, <<>>} = Kayrock.OffsetFetch.V0.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V1 response deserializes correctly with metadata" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(1)
      {actual, <<>>} = Kayrock.OffsetFetch.V1.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V2 response deserializes correctly with group error_code" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(2)
      {actual, <<>>} = Kayrock.OffsetFetch.V2.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V3 response deserializes correctly with throttle_time" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(3)
      {actual, <<>>} = Kayrock.OffsetFetch.V3.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V4 response deserializes correctly" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(4)
      {actual, <<>>} = Kayrock.OffsetFetch.V4.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V5 response deserializes correctly with committed_leader_epoch" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(5)
      {actual, <<>>} = Kayrock.OffsetFetch.V5.Response.deserialize(response_binary)
      assert actual == expected_struct
    end

    test "V6 response deserializes correctly (flexible)" do
      {response_binary, expected_struct} = OffsetFetchFactory.response_data(6)
      {actual, <<>>} = Kayrock.OffsetFetch.V6.Response.deserialize(response_binary)
      assert actual == expected_struct
    end
  end

  # ============================================
  # Error Response Tests
  # ============================================

  describe "error responses" do
    test "V0 response with error code" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # topics
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
        # partition_index
        0,
        0,
        0,
        0,
        # offset (-1 = no offset)
        255,
        255,
        255,
        255,
        255,
        255,
        255,
        255,
        # metadata (null)
        255,
        255,
        # error_code (3 = UNKNOWN_TOPIC_OR_PARTITION)
        0,
        3
      >>

      {response, <<>>} = Kayrock.OffsetFetch.V0.Response.deserialize(response_binary)

      [topic] = response.topics
      [partition] = topic.partitions
      assert partition.committed_offset == -1
      assert partition.error_code == 3
    end

    test "V2 response with group-level error" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        3,
        # topics (empty)
        0,
        0,
        0,
        0,
        # error_code (16 = NOT_COORDINATOR)
        0,
        16
      >>

      {response, <<>>} = Kayrock.OffsetFetch.V2.Response.deserialize(response_binary)
      assert response.error_code == 16
    end
  end

  # ============================================
  # Version Compatibility Tests
  # ============================================

  describe "version compatibility" do
    test "all versions serialize correctly" do
      for version <- api_version_range(:offset_fetch) do
        module = Module.concat([Kayrock, OffsetFetch, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        {request, expected_binary} = OffsetFetchFactory.request_data(version)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request serialization mismatch"

        <<api_key::16, api_version::16, _rest::binary>> = serialized
        assert api_key == 9
        assert api_version == version
      end
    end

    test "all versions deserialize correctly" do
      for version <- api_version_range(:offset_fetch) do
        module = Module.concat([Kayrock, OffsetFetch, :"V#{version}", Response])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

        {response_binary, expected_struct} = OffsetFetchFactory.response_data(version)
        {actual, <<>>} = module.deserialize(response_binary)

        assert actual == expected_struct,
               "V#{version} response deserialization mismatch"
      end
    end
  end

  # ============================================
  # Critical Edge Cases
  # ============================================

  describe "truncated binary handling" do
    test "V0 response handles truncated binary" do
      {response_binary, _} = OffsetFetchFactory.response_data(0)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.OffsetFetch.V0.Response,
          response_binary,
          truncate_at
        )
      end
    end

    test "V2 response handles truncated binary" do
      {response_binary, _} = OffsetFetchFactory.response_data(2)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.OffsetFetch.V2.Response,
          response_binary,
          truncate_at
        )
      end
    end

    test "V3 response handles truncated binary" do
      {response_binary, _} = OffsetFetchFactory.response_data(3)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.OffsetFetch.V3.Response,
          response_binary,
          truncate_at
        )
      end
    end

    test "V5 response handles truncated binary" do
      {response_binary, _} = OffsetFetchFactory.response_data(5)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.OffsetFetch.V5.Response,
          response_binary,
          truncate_at
        )
      end
    end

    test "V6 response handles truncated binary (flexible)" do
      {response_binary, _} = OffsetFetchFactory.response_data(6)

      for truncate_at <- truncation_points(response_binary) do
        assert_truncated_error(
          Kayrock.OffsetFetch.V6.Response,
          response_binary,
          truncate_at
        )
      end
    end
  end

  describe "extra bytes handling" do
    test "V0 response handles extra trailing bytes" do
      {response_binary, _} = OffsetFetchFactory.response_data(0)

      assert_extra_bytes_returned(
        Kayrock.OffsetFetch.V0.Response,
        response_binary,
        <<55, 66, 77>>
      )
    end

    test "V2 response handles extra trailing bytes" do
      {response_binary, _} = OffsetFetchFactory.response_data(2)

      assert_extra_bytes_returned(
        Kayrock.OffsetFetch.V2.Response,
        response_binary,
        <<55, 66, 77>>
      )
    end

    test "V3 response handles extra trailing bytes" do
      {response_binary, _} = OffsetFetchFactory.response_data(3)

      assert_extra_bytes_returned(
        Kayrock.OffsetFetch.V3.Response,
        response_binary,
        <<55, 66, 77>>
      )
    end

    test "V5 response handles extra trailing bytes" do
      {response_binary, _} = OffsetFetchFactory.response_data(5)

      assert_extra_bytes_returned(
        Kayrock.OffsetFetch.V5.Response,
        response_binary,
        <<55, 66, 77>>
      )
    end

    test "V6 response handles extra trailing bytes (flexible)" do
      {response_binary, _} = OffsetFetchFactory.response_data(6)

      assert_extra_bytes_returned(
        Kayrock.OffsetFetch.V6.Response,
        response_binary,
        <<55, 66, 77>>
      )
    end
  end

  describe "malformed response handling" do
    test "empty binary fails for all versions" do
      for version <- api_version_range(:offset_fetch) do
        module = Module.concat([Kayrock, OffsetFetch, :"V#{version}", Response])

        assert_raise MatchError, fn ->
          module.deserialize(<<>>)
        end
      end
    end

    test "V2 response with invalid error_code position" do
      # Missing group-level error_code at end
      incomplete = <<
        0,
        0,
        0,
        2,
        0,
        0,
        0,
        0
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.OffsetFetch.V2.Response.deserialize(incomplete)
      end
    end

    test "V3 response with missing throttle_time_ms" do
      # Missing throttle_time_ms field
      incomplete = <<
        0,
        0,
        0,
        3
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.OffsetFetch.V3.Response.deserialize(incomplete)
      end
    end

    test "V5 response with missing committed_leader_epoch" do
      # Topic and partition without committed_leader_epoch
      incomplete = <<
        # correlation_id
        0,
        0,
        0,
        5,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # topics array length (1)
        0,
        0,
        0,
        1,
        # topic name
        0,
        5,
        "topic"::binary,
        # partitions array length (1)
        0,
        0,
        0,
        1,
        # partition_index
        0,
        0,
        0,
        0,
        # committed_offset
        0,
        0,
        0,
        0,
        0,
        0,
        0,
        100
        # missing committed_leader_epoch, metadata, and error_code
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.OffsetFetch.V5.Response.deserialize(incomplete)
      end
    end

    test "V6 response with invalid compact array format" do
      # Invalid compact array marker
      incomplete = <<
        # correlation_id
        0,
        0,
        0,
        6,
        # response header tagged_fields
        0,
        # throttle_time_ms
        0,
        0,
        0,
        0,
        # topics compact array (invalid varint)
        255
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.OffsetFetch.V6.Response.deserialize(incomplete)
      end
    end
  end
end
