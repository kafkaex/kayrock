defmodule Kayrock.MetadataTest do
  use ExUnit.Case, async: true

  import Kayrock.TestSupport
  alias Kayrock.Test.Factories.MetadataFactory

  @versions api_version_range(:metadata)

  describe "versions compatibility" do
    # Test every version with real binaries
    for version <- @versions do
      test "serializes version #{version} request with all fields matching expected binary" do
        version = unquote(version)

        # Get request struct and expected binary from factory
        {request, expected_binary} = MetadataFactory.request_data(version)

        # Serialize and compare
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

        assert serialized == expected_binary,
               "V#{version} request binary mismatch:\n#{compare_binaries(serialized, expected_binary)}"

        # Verify request fields are correctly set
        assert request.correlation_id == version
        assert request.client_id == "kayrock"

        # Version-specific field validations
        if version >= 4 do
          assert Map.has_key?(request, :allow_auto_topic_creation),
                 "V#{version} should have allow_auto_topic_creation field"
        end

        if version >= 8 do
          assert Map.has_key?(request, :include_cluster_authorized_operations),
                 "V#{version} should have include_cluster_authorized_operations field"

          assert Map.has_key?(request, :include_topic_authorized_operations),
                 "V#{version} should have include_topic_authorized_operations field"
        end

        # Verify serialized binary structure
        <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized
        assert api_key == 3, "api_key should be 3 (Metadata)"
        assert api_version == version, "api_version should match version #{version}"
        assert correlation_id == version, "correlation_id should be #{version}"
      end

      test "deserializes version #{version} response with all fields validated" do
        version = unquote(version)
        response_module = Module.concat([Kayrock.Metadata, :"V#{version}", Response])

        # Get response binary and expected struct from factory
        {response_binary, expected_struct} = MetadataFactory.response_data(version)

        # Deserialize response
        {actual_struct, rest} = response_module.deserialize(response_binary)

        # Verify no trailing bytes in captured response
        assert rest == <<>>, "Captured response should have no trailing bytes"

        # Test every field matches expected struct
        assert actual_struct == expected_struct,
               "V#{version} response struct mismatch"

        # Additional field-level validations
        assert actual_struct.correlation_id == version,
               "correlation_id should match version #{version}"

        # Version-specific field validations
        if version >= 1 do
          assert Map.has_key?(actual_struct, :controller_id),
                 "V#{version} should have controller_id field"
        end

        if version >= 2 do
          assert Map.has_key?(actual_struct, :cluster_id),
                 "V#{version} should have cluster_id field"
        end

        if version >= 3 do
          assert Map.has_key?(actual_struct, :throttle_time_ms),
                 "V#{version} should have throttle_time_ms field"
        end

        if version >= 8 do
          assert Map.has_key?(actual_struct, :cluster_authorized_operations),
                 "V#{version} should have cluster_authorized_operations field"
        end
      end
    end
  end

  describe "edge cases" do
    test "handles multiple brokers in response" do
      response_binary =
        MetadataFactory.response_binary(0,
          correlation_id: 0,
          brokers: [
            %{node_id: 0, host: "broker1", port: 9092},
            %{node_id: 1, host: "broker2", port: 9093},
            %{node_id: 2, host: "broker3", port: 9094}
          ]
        )

      {response, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)
      assert length(response.brokers) == 3
      assert Enum.map(response.brokers, & &1.node_id) == [0, 1, 2]
      assert Enum.map(response.brokers, & &1.port) == [9092, 9093, 9094]
    end

    test "handles multiple topics in response" do
      response_binary =
        MetadataFactory.response_binary(0,
          correlation_id: 0,
          topics: [
            %{name: "topic-1", partitions: []},
            %{name: "topic-2", partitions: []},
            %{name: "topic-3", partitions: []}
          ]
        )

      {response, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)
      assert length(response.topics) == 3
      assert Enum.map(response.topics, & &1.name) == ["topic-1", "topic-2", "topic-3"]
    end

    test "handles topic with multiple partitions" do
      response_binary =
        MetadataFactory.response_binary(0,
          correlation_id: 0,
          topics: [
            %{
              name: "multi-partition-topic",
              partitions: [
                %{partition_index: 0, leader_id: 0, replica_nodes: [0, 1], isr_nodes: [0, 1]},
                %{partition_index: 1, leader_id: 1, replica_nodes: [1, 2], isr_nodes: [1, 2]},
                %{partition_index: 2, leader_id: 2, replica_nodes: [2, 0], isr_nodes: [2, 0]}
              ]
            }
          ]
        )

      {response, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)
      [topic] = response.topics
      assert length(topic.partitions) == 3
      assert Enum.map(topic.partitions, & &1.partition_index) == [0, 1, 2]
      assert Enum.map(topic.partitions, & &1.leader_id) == [0, 1, 2]
    end

    test "handles error response with error code" do
      response_binary =
        MetadataFactory.error_response(0,
          error_code: 3,
          topic_name: "unknown-topic",
          correlation_id: 5
        )

      {response, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)
      assert response.correlation_id == 5
      [topic] = response.topics
      assert topic.error_code == 3
      assert topic.name == "unknown-topic"
    end

    test "V1 handles null topics (all topics)" do
      request = %Kayrock.Metadata.V1.Request{
        correlation_id: 1,
        client_id: "test",
        topics: nil
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      # Verify null is encoded as -1
      assert String.contains?(serialized, <<255, 255, 255, 255>>)
    end

    test "V2 handles cluster_id in response" do
      response_binary =
        MetadataFactory.response_binary(2,
          correlation_id: 2,
          cluster_id: "test-cluster-123"
        )

      {response, <<>>} = Kayrock.Metadata.V2.Response.deserialize(response_binary)
      assert response.cluster_id == "test-cluster-123"
    end

    test "V3 handles throttle_time_ms in response" do
      response_binary =
        MetadataFactory.response_binary(3,
          correlation_id: 3,
          throttle_time_ms: 100
        )

      {response, <<>>} = Kayrock.Metadata.V3.Response.deserialize(response_binary)
      assert response.throttle_time_ms == 100
    end

    test "V4 handles allow_auto_topic_creation in request" do
      {request, _} = MetadataFactory.request_data(4)
      assert request.allow_auto_topic_creation == true

      # Test with false
      request_false = %Kayrock.Metadata.V4.Request{
        correlation_id: 4,
        client_id: "test",
        topics: [],
        allow_auto_topic_creation: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request_false))
      assert is_binary(serialized)
      # Last byte should be 0 (false)
      assert :binary.last(serialized) == 0
    end

    test "V8 handles authorized_operations flags in request" do
      request = %Kayrock.Metadata.V8.Request{
        correlation_id: 8,
        client_id: "test",
        topics: [],
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: true,
        include_topic_authorized_operations: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      # Last 2 bytes should be 1 (true, true)
      <<_rest::binary-size(byte_size(serialized) - 2), flag1::8, flag2::8>> = serialized
      assert flag1 == 1
      assert flag2 == 1
    end

    test "V8 handles cluster_authorized_operations in response" do
      response_binary =
        MetadataFactory.response_binary(8,
          correlation_id: 8,
          cluster_authorized_operations: 255
        )

      {response, <<>>} = Kayrock.Metadata.V8.Response.deserialize(response_binary)
      assert response.cluster_authorized_operations == 255
    end

    test "V9 compact format handles empty arrays" do
      {request, expected_binary} = MetadataFactory.request_data(9)
      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))

      assert serialized == expected_binary,
             "V9 compact format mismatch:\n#{compare_binaries(serialized, expected_binary)}"
    end

    test "request with many topics (50) serializes correctly" do
      topics = for i <- 1..50, do: %{name: "topic-#{i}"}

      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "test",
        topics: topics
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      assert String.contains?(serialized, "topic-1")
      assert String.contains?(serialized, "topic-50")
    end

    test "internal topic (__consumer_offsets) serializes correctly" do
      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: "__consumer_offsets"}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "__consumer_offsets")
    end

    test "topic with special characters serializes correctly" do
      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: "my.topic-name_123"}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "my.topic-name_123")
    end

    test "topic with max length name (249 chars) serializes correctly" do
      long_name = String.duplicate("a", 249)

      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: long_name}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, long_name)
    end

    test "handles truncated binary at various points for all versions" do
      for version <- @versions do
        response_module = Module.concat([Kayrock.Metadata, :"V#{version}", Response])
        {response_binary, _expected_struct} = MetadataFactory.response_data(version)

        for truncate_at <- truncation_points(response_binary) do
          assert_truncated_error(response_module, response_binary, truncate_at)
        end
      end
    end

    test "handles extra trailing bytes for all versions" do
      for version <- @versions do
        response_module = Module.concat([Kayrock.Metadata, :"V#{version}", Response])
        {response_binary, _expected_struct} = MetadataFactory.response_data(version)

        extra_bytes = <<11, 22, 33, 44>>

        assert_extra_bytes_returned(
          response_module,
          response_binary,
          extra_bytes
        )
      end
    end

    test "empty binary fails to deserialize" do
      assert_raise MatchError, fn ->
        Kayrock.Metadata.V0.Response.deserialize(<<>>)
      end
    end

    test "invalid broker count fails" do
      invalid = <<
        # correlation_id
        0,
        0,
        0,
        0,
        # Claims 1000 brokers but no data
        0,
        0,
        3,
        232
      >>

      assert_raise FunctionClauseError, fn ->
        Kayrock.Metadata.V0.Response.deserialize(invalid)
      end
    end

    test "response with only correlation_id fails" do
      partial = <<0, 0, 0, 0>>

      assert_raise MatchError, fn ->
        Kayrock.Metadata.V0.Response.deserialize(partial)
      end
    end
  end

  describe "integration with Request protocol" do
    test "api_vsn returns correct version for all versions" do
      for version <- @versions do
        request_module = Module.concat([Kayrock.Metadata, :"V#{version}", Request])

        base_fields = %{
          correlation_id: version,
          client_id: "test",
          topics: []
        }

        fields =
          cond do
            version >= 8 ->
              Map.merge(base_fields, %{
                allow_auto_topic_creation: true,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false
              })

            version >= 4 ->
              Map.merge(base_fields, %{allow_auto_topic_creation: true})

            true ->
              base_fields
          end

        request = struct(request_module, fields)

        assert Kayrock.Request.api_vsn(request) == version,
               "api_vsn should return #{version} for V#{version}.Request"
      end
    end

    test "response_deserializer returns correct function for all versions" do
      for version <- @versions do
        request_module = Module.concat([Kayrock.Metadata, :"V#{version}", Request])

        base_fields = %{
          correlation_id: version,
          client_id: "test",
          topics: []
        }

        fields =
          cond do
            version >= 8 ->
              Map.merge(base_fields, %{
                allow_auto_topic_creation: true,
                include_cluster_authorized_operations: false,
                include_topic_authorized_operations: false
              })

            version >= 4 ->
              Map.merge(base_fields, %{allow_auto_topic_creation: true})

            true ->
              base_fields
          end

        request = struct(request_module, fields)

        deserializer = Kayrock.Request.response_deserializer(request)
        assert is_function(deserializer, 1), "V#{version} should have a deserializer function"
      end
    end
  end
end
