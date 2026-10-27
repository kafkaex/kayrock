defmodule Kayrock.MetadataTopicIdTest do
  @moduledoc """
  Metadata v10-v13 (JSON-sourced; v0-v9 come from `:kpro_schema` and are
  covered by `Kayrock.MetadataTest`).

  Wire fixtures are derived by hand from the Apache Kafka 4.4.0
  `MetadataRequest.json` / `MetadataResponse.json` specs:

    * v10 adds `topic_id` (uuid) to request and response topics and makes the
      request topic `name` nullable;
    * v11 drops `include_cluster_authorized_operations` (request) and
      `cluster_authorized_operations` (response);
    * v12 makes the response topic `name` nullable (lookup by topic id);
    * v13 adds a top-level `error_code` to the response.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Metadata

  @topic_id <<0xC0, 0xFF, 0xEE, 0x00, 0x11, 0x22, 0x43, 0x33, 0x84, 0x44, 0x55, 0x66, 0x77, 0x88,
              0x99, 0xAA>>

  defp request_header(vsn), do: <<3::16, vsn::16, 5::32, 7::16, "kayrock", 0>>

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  test "version range and dispatch" do
    assert Metadata.min_vsn() == 0
    assert Metadata.max_vsn() == 13

    for vsn <- 10..13 do
      assert Kayrock.Request.api_vsn(Metadata.get_request_struct(vsn)) == vsn
    end
  end

  describe "requests" do
    test "V10 by name (topic_id zero UUID, cluster authorized operations flag)" do
      request = %Metadata.V10.Request{
        correlation_id: 5,
        client_id: "kayrock",
        topics: [%{topic_id: nil, name: "t1", tagged_fields: []}],
        allow_auto_topic_creation: false,
        include_cluster_authorized_operations: true,
        include_topic_authorized_operations: false
      }

      expected =
        request_header(10) <>
          <<
            # topics: 1 element
            2,
            # topic_id: nil -> zero UUID
            0::128,
            # name "t1"
            3,
            "t1",
            # topic tag buffer
            0,
            # allow_auto_topic_creation, include_cluster_authorized_operations,
            # include_topic_authorized_operations
            0,
            1,
            0,
            0
          >>

      assert serialize(request) == expected
    end

    test "V11 drops include_cluster_authorized_operations" do
      request = %Metadata.V11.Request{
        correlation_id: 5,
        client_id: "kayrock",
        topics: nil,
        allow_auto_topic_creation: true,
        include_topic_authorized_operations: true
      }

      refute Map.has_key?(request, :include_cluster_authorized_operations)
      # topics null (all topics), allow_auto_topic_creation, include_topic_authorized_operations
      assert serialize(request) == request_header(11) <> <<0, 1, 1, 0>>
    end

    for vsn <- [12, 13] do
      test "V#{vsn} by topic id with a null name" do
        vsn = unquote(vsn)

        request = %{
          Metadata.get_request_struct(vsn)
          | correlation_id: 5,
            client_id: "kayrock",
            topics: [%{topic_id: @topic_id, name: nil, tagged_fields: []}],
            allow_auto_topic_creation: false,
            include_topic_authorized_operations: false
        }

        expected = request_header(vsn) <> <<2>> <> @topic_id <> <<0, 0, 0, 0, 0>>
        assert serialize(request) == expected
      end
    end
  end

  # Response body up to (not including) the topic list
  defp brokers_section do
    <<
      # throttle_time_ms
      0::32,
      # brokers: [{1, "localhost", 9092, rack null}]
      2,
      1::32,
      10,
      "localhost",
      9092::32,
      0,
      0,
      # cluster_id "c"
      2,
      "c",
      # controller_id
      1::32
    >>
  end

  # One topic (name given as encoded compact (nullable) string bytes)
  defp topic_section(name_bytes) do
    IO.iodata_to_binary([
      # topics: 1 element; error_code 0
      <<2, 0::16>>,
      name_bytes,
      @topic_id,
      # is_internal false
      <<0>>,
      # partitions: [{0, 0, leader 1, epoch 0, [1], [1], []}]
      <<2, 0::16, 0::32, 1::32, 0::32, 2, 1::32, 2, 1::32, 1, 0>>,
      # topic_authorized_operations, topic tag buffer
      <<-2_147_483_648::32-signed, 0>>
    ])
  end

  @partition %{
    error_code: 0,
    partition_index: 0,
    leader_id: 1,
    leader_epoch: 0,
    replica_nodes: [1],
    isr_nodes: [1],
    offline_replicas: [],
    tagged_fields: []
  }

  defp expected_topic(name) do
    %{
      error_code: 0,
      name: name,
      topic_id: @topic_id,
      # Kayrock decodes booleans as 0 / 1
      is_internal: 0,
      partitions: [@partition],
      topic_authorized_operations: -2_147_483_648,
      tagged_fields: []
    }
  end

  @broker %{node_id: 1, host: "localhost", port: 9092, rack: nil, tagged_fields: []}

  describe "responses" do
    test "V10 carries topic_id and cluster_authorized_operations" do
      data =
        <<5::32, 0>> <>
          brokers_section() <>
          topic_section(<<3, "t1">>) <> <<-2_147_483_648::32-signed, 0>>

      assert {response, <<>>} = Metadata.V10.Response.deserialize(data)

      assert response == %Metadata.V10.Response{
               correlation_id: 5,
               throttle_time_ms: 0,
               brokers: [@broker],
               cluster_id: "c",
               controller_id: 1,
               topics: [expected_topic("t1")],
               cluster_authorized_operations: -2_147_483_648,
               tagged_fields: []
             }
    end

    test "V11 has no cluster_authorized_operations" do
      data = <<5::32, 0>> <> brokers_section() <> topic_section(<<3, "t1">>) <> <<0>>

      assert {response, <<>>} = Metadata.deserialize(11, data)
      refute Map.has_key?(response, :cluster_authorized_operations)
      assert response.topics == [expected_topic("t1")]
    end

    test "V12 topic name may be null" do
      data = <<5::32, 0>> <> brokers_section() <> topic_section(<<0>>) <> <<0>>

      assert {%Metadata.V12.Response{topics: [topic]}, <<>>} = Metadata.deserialize(12, data)
      assert topic == expected_topic(nil)
    end

    test "V13 adds a top-level error_code" do
      data = <<5::32, 0>> <> brokers_section() <> topic_section(<<3, "t1">>) <> <<0::16, 0>>

      assert {response, <<>>} = Metadata.V13.Response.deserialize(data)
      assert response.error_code == 0
      assert response.topics == [expected_topic("t1")]

      # e.g. REBOOTSTRAP_REQUIRED (129) with no brokers/topics
      error = <<5::32, 0, 0::32, 1, 0, -1::32-signed, 1, 129::16, 0>>

      assert {%{error_code: 129, brokers: [], cluster_id: nil, topics: []}, <<>>} =
               Metadata.V13.Response.deserialize(error)
    end
  end
end
