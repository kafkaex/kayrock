defmodule Kayrock.Apis.ApiVersionsTest do
  @moduledoc """
  Tests for ApiVersions API (V0-V3).

  Binary data captured from Confluent Kafka 7.4.0 (Kafka protocol 3.4).

  Protocol structure:
  - V0-V2: Standard request/response format
  - V3: Flexible/compact format (not fully supported by broker 7.4.0)
  """
  use ExUnit.Case, async: true

  # Request binaries captured from real Kafka interaction
  # Client ID: "kayrock-capture", correlation_id matches version number

  describe "V0" do
    alias Kayrock.ApiVersions.V0.Request
    alias Kayrock.ApiVersions.V0.Response

    # Captured from Kafka 7.4.0
    @v0_request_binary <<
      # api_key (18 = ApiVersions)
      0,
      18,
      # api_version (0)
      0,
      0,
      # correlation_id (0)
      0,
      0,
      0,
      0,
      # client_id length (15)
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    test "serializes request matching captured binary" do
      request = %Request{
        correlation_id: 0,
        client_id: "kayrock-capture"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == @v0_request_binary
    end

    test "serializes request with different client_id" do
      request = %Request{
        correlation_id: 1,
        client_id: "test"
      }

      expected = <<
        # api_key
        0,
        18,
        # api_version
        0,
        0,
        # correlation_id
        0,
        0,
        0,
        1,
        # client_id length
        0,
        4,
        # client_id
        "test"::binary
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    test "deserializes response prefix" do
      # Use minimal response for testing structure
      response_binary = <<
        # correlation_id (42)
        0,
        0,
        0,
        42,
        # error_code (0)
        0,
        0,
        # api_keys array length (2)
        0,
        0,
        0,
        2,
        # Produce: api_key=0, min=0, max=9
        0,
        0,
        0,
        0,
        0,
        9,
        # Fetch: api_key=1, min=0, max=13
        0,
        1,
        0,
        0,
        0,
        13
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 42
      assert response.error_code == 0
      assert length(response.api_keys) == 2

      [produce, fetch] = response.api_keys
      assert produce == %{api_key: 0, min_version: 0, max_version: 9}
      assert fetch == %{api_key: 1, min_version: 0, max_version: 13}
    end

    test "deserializes real Kafka response (first 10 APIs)" do
      # Real response prefix from Kafka 7.4.0
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # 10 APIs
        0,
        0,
        0,
        10,
        # Produce V0-9
        0,
        0,
        0,
        0,
        0,
        9,
        # Fetch V0-13
        0,
        1,
        0,
        0,
        0,
        13,
        # ListOffsets V0-7
        0,
        2,
        0,
        0,
        0,
        7,
        # Metadata V0-12
        0,
        3,
        0,
        0,
        0,
        12,
        # LeaderAndIsr V0-7
        0,
        4,
        0,
        0,
        0,
        7,
        # StopReplica V0-4
        0,
        5,
        0,
        0,
        0,
        4,
        # UpdateMetadata V0-8
        0,
        6,
        0,
        0,
        0,
        8,
        # ControlledShutdown V0-3
        0,
        7,
        0,
        0,
        0,
        3,
        # OffsetCommit V0-8
        0,
        8,
        0,
        0,
        0,
        8,
        # OffsetFetch V0-8
        0,
        9,
        0,
        0,
        0,
        8
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 0
      assert response.error_code == 0
      assert length(response.api_keys) == 10

      # Verify specific APIs
      api_map = Map.new(response.api_keys, &{&1.api_key, &1})

      # Produce
      assert api_map[0].max_version == 9
      # Fetch
      assert api_map[1].max_version == 13
      # Metadata
      assert api_map[3].max_version == 12
      # OffsetCommit
      assert api_map[8].max_version == 8
      # OffsetFetch
      assert api_map[9].max_version == 8
    end

    test "deserializes empty api_keys" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # empty array
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.api_keys == []
    end

    test "deserializes error response" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code (UNSUPPORTED_VERSION)
        0,
        35,
        # empty api_keys
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)
      assert response.error_code == 35
    end
  end

  describe "V1" do
    alias Kayrock.ApiVersions.V1.Request
    alias Kayrock.ApiVersions.V1.Response

    @v1_request_binary <<
      # api_key
      0,
      18,
      # api_version (1)
      0,
      1,
      # correlation_id (1)
      0,
      0,
      0,
      1,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    test "serializes request matching captured binary" do
      request = %Request{
        correlation_id: 1,
        client_id: "kayrock-capture"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == @v1_request_binary
    end

    test "deserializes response with throttle_time_ms" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        1,
        # error_code
        0,
        0,
        # api_keys count
        0,
        0,
        0,
        2,
        # Produce
        0,
        0,
        0,
        0,
        0,
        9,
        # Fetch
        0,
        1,
        0,
        0,
        0,
        13,
        # throttle_time_ms (0)
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 1
      assert response.error_code == 0
      assert response.throttle_time_ms == 0
      assert length(response.api_keys) == 2
    end

    test "deserializes response with non-zero throttle" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        5,
        # error_code
        0,
        0,
        # api_keys count
        0,
        0,
        0,
        1,
        # ApiVersions V0-3
        0,
        18,
        0,
        0,
        0,
        3,
        # throttle_time_ms (100ms)
        0,
        0,
        0,
        100
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 5
      assert response.throttle_time_ms == 100
      assert [api_versions] = response.api_keys
      assert api_versions.api_key == 18
      assert api_versions.max_version == 3
    end
  end

  describe "V2" do
    alias Kayrock.ApiVersions.V2.Request
    alias Kayrock.ApiVersions.V2.Response

    @v2_request_binary <<
      # api_key
      0,
      18,
      # api_version (2)
      0,
      2,
      # correlation_id (2)
      0,
      0,
      0,
      2,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary
    >>

    test "serializes request matching captured binary" do
      request = %Request{
        correlation_id: 2,
        client_id: "kayrock-capture"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == @v2_request_binary
    end

    test "deserializes response (same as V1)" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        2,
        # error_code
        0,
        0,
        # empty api_keys
        0,
        0,
        0,
        0,
        # throttle_time_ms (50)
        0,
        0,
        0,
        50
      >>

      {response, <<>>} = Response.deserialize(response_binary)

      assert response.correlation_id == 2
      assert response.throttle_time_ms == 50
    end
  end

  describe "V3" do
    alias Kayrock.ApiVersions.V3.Request

    # Note: V3 uses flexible/compact format. Kafka 7.4.0 returns error 61
    # (UNSUPPORTED_VERSION) for V3 requests in some cases.

    @v3_request_binary <<
      # api_key
      0,
      18,
      # api_version (3)
      0,
      3,
      # correlation_id (3)
      0,
      0,
      0,
      3,
      # client_id length
      0,
      15,
      # client_id
      "kayrock-capture"::binary,
      # flexible header marker
      0,
      # client_software_name length+1 (7+1)
      8,
      # client_software_name
      "kayrock"::binary,
      # client_software_version length+1 (5+1)
      6,
      # client_software_version
      "1.0.0"::binary,
      # tagged_fields count
      0
    >>

    test "serializes request with compact format" do
      request = %Request{
        correlation_id: 3,
        client_id: "kayrock-capture",
        client_software_name: "kayrock",
        client_software_version: "1.0.0"
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == @v3_request_binary
    end

    test "serializes request with empty fields" do
      request = %Request{
        correlation_id: 0,
        client_id: "",
        client_software_name: "",
        client_software_version: ""
      }

      expected = <<
        # api_key
        0,
        18,
        # api_version
        0,
        3,
        # correlation_id
        0,
        0,
        0,
        0,
        # client_id length (0)
        0,
        0,
        # flexible header marker
        0,
        # empty compact_string (0+1)
        1,
        # empty compact_string (0+1)
        1,
        # tagged_fields count
        0
      >>

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert serialized == expected
    end

    # V3 Response tests skipped - broker returns UNSUPPORTED_VERSION
    # The V3 response format uses compact arrays and tagged fields
  end

  describe "integration with Request protocol" do
    test "api_vsn returns correct version" do
      assert Kayrock.Request.api_vsn(%Kayrock.ApiVersions.V0.Request{}) == 0
      assert Kayrock.Request.api_vsn(%Kayrock.ApiVersions.V1.Request{}) == 1
      assert Kayrock.Request.api_vsn(%Kayrock.ApiVersions.V2.Request{}) == 2
      assert Kayrock.Request.api_vsn(%Kayrock.ApiVersions.V3.Request{}) == 3
    end

    test "response_deserializer returns correct function" do
      deserializer = Kayrock.Request.response_deserializer(%Kayrock.ApiVersions.V0.Request{})
      assert is_function(deserializer, 1)
    end
  end

  describe "edge cases" do
    test "handles max int16 api_key" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code
        0,
        0,
        # 1 api
        0,
        0,
        0,
        1,
        # api_key=32_767, min=0, max=32_767
        127,
        255,
        0,
        0,
        127,
        255
      >>

      {response, <<>>} = Kayrock.ApiVersions.V0.Response.deserialize(response_binary)
      [entry] = response.api_keys
      assert entry.api_key == 32_767
      assert entry.max_version == 32_767
    end

    test "handles negative error codes" do
      response_binary = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # error_code (-1 = UNKNOWN_SERVER_ERROR)
        255,
        255,
        # empty api_keys
        0,
        0,
        0,
        0
      >>

      {response, <<>>} = Kayrock.ApiVersions.V0.Response.deserialize(response_binary)
      assert response.error_code == -1
    end
  end
end
