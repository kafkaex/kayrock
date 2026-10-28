defmodule Kayrock.StreamsGroupTopologyDescriptionUpdateTest do
  @moduledoc """
  Tests for StreamsGroupTopologyDescriptionUpdate (API key 93, KIP-1331), v0.

  Generated from the Apache Kafka 4.4.0 JSON spec. v0 is flexible. A streams
  group member sends it when a StreamsGroupHeartbeat v1 response has
  `topology_description_required` set.

  Wire fixtures are derived by hand from
  `priv/kafka_messages/4.4.0/StreamsGroupTopologyDescriptionUpdate{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.StreamsGroupTopologyDescriptionUpdate
  alias Kayrock.StreamsGroupTopologyDescriptionUpdate.V0.Request
  alias Kayrock.StreamsGroupTopologyDescriptionUpdate.V0.Response

  test "version metadata" do
    assert StreamsGroupTopologyDescriptionUpdate.min_vsn() == 0
    assert StreamsGroupTopologyDescriptionUpdate.max_vsn() == 0

    assert Kayrock.KafkaSchemaMetadata.api_key(:streams_group_topology_description_update) ==
             93

    assert Kayrock.KafkaSchemaMetadata.version_range(:streams_group_topology_description_update) ==
             {0, 0}

    assert %Request{} = StreamsGroupTopologyDescriptionUpdate.get_request_struct(0)
    assert Request.api_key() == 93
    assert Request.api_vsn() == 0
  end

  describe "v0 request" do
    test "serializes a topology with a subtopology and a global store" do
      source = %{
        name: "SRC",
        node_type: 1,
        source_topics: ["in"],
        sink_topic: nil,
        stores: [],
        successors: ["SINK"]
      }

      request = %Request{
        correlation_id: 6,
        client_id: "k",
        group_id: "g",
        member_id: "m1",
        topology_epoch: 2,
        topology_description: %{
          subtopologies: [
            %{
              subtopology_id: "0",
              nodes: [
                source,
                %{
                  name: "SINK",
                  node_type: 3,
                  source_topics: [],
                  sink_topic: "out",
                  stores: [],
                  successors: []
                }
              ]
            }
          ],
          global_stores: [
            %{
              source: %{source | successors: ["P"]},
              processor: %{
                name: "P",
                node_type: 2,
                source_topics: [],
                sink_topic: nil,
                stores: ["st"],
                successors: []
              }
            }
          ]
        }
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               IO.iodata_to_binary([
                 # request header v2
                 <<93::16, 0::16, 6::32, 1::16, "k", 0>>,
                 # group_id, member_id, topology_epoch
                 <<2, "g", 3, "m1", 2::32>>,
                 # topology_description.subtopologies: [{"0", nodes: 2}]
                 <<2, 2, "0", 3>>,
                 # SRC: name, node_type, source_topics ["in"], sink_topic null,
                 # stores [], successors ["SINK"], tag buffer
                 <<4, "SRC", 1, 2, 3, "in", 0, 1, 2, 5, "SINK", 0>>,
                 # SINK: sink_topic "out"
                 <<5, "SINK", 3, 1, 4, "out", 1, 1, 0>>,
                 # subtopology tag buffer
                 <<0>>,
                 # global_stores: 1 element
                 <<2>>,
                 <<4, "SRC", 1, 2, 3, "in", 0, 1, 2, 2, "P", 0>>,
                 <<2, "P", 2, 1, 0, 2, 3, "st", 1, 0>>,
                 # global store tag buffer, topology_description tag buffer
                 <<0, 0>>,
                 # root tag buffer
                 <<0>>
               ])
    end

    test "an empty topology description" do
      request = %Request{
        correlation_id: 6,
        client_id: "k",
        group_id: "g",
        member_id: "m",
        topology_epoch: 0,
        topology_description: %{subtopologies: [], global_stores: []}
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               <<93::16, 0::16, 6::32, 1::16, "k", 0, 2, "g", 2, "m", 0::32, 1, 1, 0, 0>>
    end
  end

  describe "v0 response" do
    test "decodes success with no leftover bytes" do
      wire = <<6::32, 0, 0::32, 0::16, 0, 0>>

      assert {response, <<>>} = Response.deserialize(wire)

      assert response == %Response{
               correlation_id: 6,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               tagged_fields: []
             }
    end

    test "decodes the error a broker without a description plugin returns" do
      message = "The broker has no streams group topology description plugin configured."

      wire =
        IO.iodata_to_binary([
          <<6::32, 0, 0::32>>,
          # UNSUPPORTED_VERSION (35)
          <<35::16>>,
          <<byte_size(message) + 1>>,
          message,
          <<0>>
        ])

      assert {%Response{error_code: 35, error_message: ^message}, <<>>} =
               StreamsGroupTopologyDescriptionUpdate.deserialize(0, wire)
    end
  end
end
