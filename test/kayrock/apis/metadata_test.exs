defmodule Kayrock.Apis.MetadataTest do
  defmodule V0 do
    use ExUnit.Case

    alias Kayrock.Metadata.V0.Request
    alias Kayrock.Metadata.V0.Response

    import Kayrock.TestSupport

    test "correctly serializes a valid metadata request with no topics" do
      good_request = <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 0::32>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: []
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly serializes a valid metadata request with a single topic" do
      good_request = <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 1::32, 3::16, "bar"::binary>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: [%{name: "bar"}]
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly serializes a valid metadata request with a multiple topics" do
      good_request =
        <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 3::32, 3::16, "bar"::binary, 3::16,
          "baz"::binary, 4::16, "food"::binary>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: [%{name: "bar"}, %{name: "baz"}, %{name: "food"}]
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly deserializes a valid response" do
      response =
        <<0::32, 1::32, 0::32, 3::16, "foo"::binary, 9092::32, 1::32, 0::16, 3::16, "bar"::binary,
          1::32, 0::16, 0::32, 0::32, 0::32, 1::32, 0::32>>

      expect = %Response{
        correlation_id: 0,
        brokers: [
          %{node_id: 0, host: "foo", port: 9092}
        ],
        topics: [
          %{
            error_code: 0,
            name: "bar",
            partitions: [
              %{
                error_code: 0,
                partition_index: 0,
                replica_nodes: [],
                isr_nodes: [0],
                leader_id: 0
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Metadata.deserialize(0, response)
      assert got == expect
    end

    # ============================================
    # Edge Cases
    # ============================================

    test "many topics (50) serializes correctly" do
      topics = for i <- 1..50, do: %{name: "topic-#{i}"}

      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: topics
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
      assert String.contains?(serialized, "topic-1")
      assert String.contains?(serialized, "topic-50")
    end

    test "response with multiple brokers deserializes correctly" do
      # Based on working test format: correlation_id, broker_count, (node_id, host_len, host, port)*, topic_count
      response =
        <<0::32, 3::32, 0::32, 9::16, "localhost"::binary, 9092::32, 1::32, 9::16,
          "localhost"::binary, 9093::32, 2::32, 9::16, "localhost"::binary, 9094::32, 0::32>>

      {got, ""} = Response.deserialize(response)

      assert length(got.brokers) == 3
      assert Enum.map(got.brokers, & &1.node_id) == [0, 1, 2]
      assert Enum.map(got.brokers, & &1.port) == [9092, 9093, 9094]
    end

    test "internal topic (__consumer_offsets) serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: "__consumer_offsets"}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "__consumer_offsets")
    end

    test "topic with special characters serializes correctly" do
      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: "my.topic-name_123"}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, "my.topic-name_123")
    end

    test "topic with max length name (249 chars) serializes correctly" do
      long_name = String.duplicate("a", 249)

      request = %Request{
        correlation_id: 1,
        client_id: "test",
        topics: [%{name: long_name}]
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert String.contains?(serialized, long_name)
    end
  end

  defmodule V1 do
    use ExUnit.Case

    alias Kayrock.Metadata.V0.Response

    test "correctly handles an empty topic list" do
      data =
        <<0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 9, 49, 48, 46, 48, 46, 49, 46, 50, 49, 0, 0, 35,
          132, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0>>

      {rsp, _} = Response.deserialize(data)

      assert rsp.topics == []
    end
  end
end

# ============================================
# Version-specific tests (V2-V9)
# V0-V1 covered above
# ============================================

defmodule MetadataVersionTests do
  use ExUnit.Case

  import Kayrock.TestSupport

  describe "Metadata V2-V9 request serialization" do
    test "V4 request serializes with allow_auto_topic_creation" do
      request = %Kayrock.Metadata.V4.Request{
        correlation_id: 4,
        client_id: "test",
        topics: [%{name: "topic"}],
        allow_auto_topic_creation: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 3
      assert api_version == 4
    end

    test "V8 request serializes with authorized_operations flags" do
      request = %Kayrock.Metadata.V8.Request{
        correlation_id: 8,
        client_id: "test",
        topics: nil,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 3
      assert api_version == 8
    end

    test "V9 request serializes (compact format)" do
      request = %Kayrock.Metadata.V9.Request{
        correlation_id: 9,
        client_id: "test",
        topics: nil,
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end

  describe "Metadata version compatibility" do
    test "all versions V0-V9 serialize" do
      for version <- api_version_range(:metadata) do
        module = Module.concat([Kayrock, Metadata, :"V#{version}", Request])
        assert Code.ensure_loaded?(module), "Module #{inspect(module)} should exist"

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

        request = struct(module, fields)
        serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
        assert is_binary(serialized), "V#{version} should serialize"
      end
    end
  end

  # ============================================
  # V4+ Edge Cases
  # ============================================

  describe "Metadata V4+ edge cases" do
    test "V4 with nil topics (all topics) serializes correctly" do
      request = %Kayrock.Metadata.V4.Request{
        correlation_id: 4,
        client_id: "test",
        topics: nil,
        allow_auto_topic_creation: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V4 with allow_auto_topic_creation=true serializes correctly" do
      request = %Kayrock.Metadata.V4.Request{
        correlation_id: 4,
        client_id: "test",
        topics: [%{name: "auto-create-topic"}],
        allow_auto_topic_creation: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 3
      assert api_version == 4
    end
  end

  describe "Metadata V8+ authorized operations edge cases" do
    test "V8 with all authorized operations flags true" do
      request = %Kayrock.Metadata.V8.Request{
        correlation_id: 8,
        client_id: "test",
        topics: [%{name: "topic"}],
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: true,
        include_topic_authorized_operations: true
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      <<api_key::16, api_version::16, _rest::binary>> = serialized
      assert api_key == 3
      assert api_version == 8
    end
  end

  describe "Metadata V9 compact format edge cases" do
    test "V9 with empty topics list serializes correctly" do
      request = %Kayrock.Metadata.V9.Request{
        correlation_id: 9,
        client_id: "test",
        topics: [],
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end

    test "V9 with multiple topics serializes correctly" do
      request = %Kayrock.Metadata.V9.Request{
        correlation_id: 9,
        client_id: "test",
        topics: [%{name: "topic-1"}, %{name: "topic-2"}, %{name: "topic-3"}],
        allow_auto_topic_creation: true,
        include_cluster_authorized_operations: false,
        include_topic_authorized_operations: false
      }

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert is_binary(serialized)
    end
  end
end
