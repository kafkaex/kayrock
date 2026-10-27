defmodule Kayrock.StreamsGroupDescribeTest do
  @moduledoc """
  Tests for StreamsGroupDescribe (API key 89, KIP-1071), v0-v1.

  Generated from the Apache Kafka 4.4.0 JSON spec. Both versions are
  flexible. v1 adds `include_topology_description` to the request and
  `topology_description`, `topology_description_status` (KIP-1331) and
  `assignor_name` (KIP-1357) to each described group.

  The v1 response uses the field name `subtopologies` for two different
  nested structs (`topology.subtopologies` and
  `topology_description.subtopologies`), and `active_tasks`, `topic_configs`,
  ... for structs reached by several paths; the "nested scopes" tests below
  check each one decodes with its own schema.

  Wire fixtures are derived by hand from
  `priv/kafka_messages/4.4.0/StreamsGroupDescribe{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.StreamsGroupDescribe

  test "version metadata" do
    assert StreamsGroupDescribe.min_vsn() == 0
    assert StreamsGroupDescribe.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:streams_group_describe) == 89
    assert Kayrock.KafkaSchemaMetadata.version_range(:streams_group_describe) == {0, 1}

    for vsn <- 0..1 do
      request = StreamsGroupDescribe.get_request_struct(vsn)
      assert request.__struct__.api_key() == 89
      assert request.__struct__.api_vsn() == vsn
    end
  end

  describe "requests" do
    test "v0" do
      request = %StreamsGroupDescribe.V0.Request{
        correlation_id: 8,
        client_id: "k",
        group_ids: ["g1", "g2"],
        include_authorized_operations: true
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               IO.iodata_to_binary([
                 # request header v2
                 <<89::16, 0::16, 8::32, 1::16, "k", 0>>,
                 # group_ids ["g1", "g2"]
                 <<3, 3, "g1", 3, "g2">>,
                 # include_authorized_operations, tag buffer
                 <<1, 0>>
               ])
    end

    test "v1 adds include_topology_description" do
      request = %StreamsGroupDescribe.V1.Request{
        correlation_id: 7,
        client_id: "k",
        group_ids: ["g"],
        include_authorized_operations: false,
        include_topology_description: true
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               <<89::16, 1::16, 7::32, 1::16, "k", 0, 2, 2, "g", 0, 1, 0>>
    end
  end

  # One described group, error-free, with a topology and one member; shared
  # by v0 and v1 (v1 appends its three fields to the group).
  defp group_head do
    [
      # error_code, error_message: null
      <<0::16, 0>>,
      # group_id "app", group_state "stable", group_epoch, assignment_epoch
      <<4, "app", 7, "stable", 3::32, 3::32>>,
      # topology: present, epoch 1
      <<1, 1::32>>,
      # topology.subtopologies: 1 element
      <<2, 2, "0">>,
      # source_topics ["in"], repartition_sink_topics []
      <<2, 3, "in", 1>>,
      # state_changelog_topics: [{"app-s-changelog", 2, 1, [{"cleanup.policy", "compact"}]}]
      <<2, 16, "app-s-changelog", 2::32, 1::16>>,
      <<2, 15, "cleanup.policy", 8, "compact", 0, 0>>,
      # repartition_source_topics: [{"app-r", 4, 3, []}]
      <<2, 6, "app-r", 4::32, 3::16, 1, 0>>,
      # subtopology tag buffer, topology tag buffer
      <<0, 0>>,
      # members: 1 element
      <<2>>,
      # member_id, member_epoch, instance_id null, rack_id "r1"
      <<3, "m1", 3::32, 0, 3, "r1">>,
      # client_id, client_host
      <<4, "cid", 11, "/127.0.0.1">>,
      # topology_epoch, process_id
      <<1::32, 5, "proc">>,
      # user_endpoint: present localhost:8080
      <<1, 10, "localhost", 8080::16, 0>>,
      # client_tags [{"zone", "a"}]
      <<2, 5, "zone", 2, "a", 0>>,
      # task_offsets [{"0", 0, 10}], task_end_offsets [{"0", 0, 12}]
      <<2, 2, "0", 0::32, 10::64, 0>>,
      <<2, 2, "0", 0::32, 12::64, 0>>,
      # assignment: active [{"0", [0]}], standby [], warmup []
      <<2, 2, "0", 2, 0::32, 0, 1, 1, 0>>,
      # target_assignment: active [], standby [{"0", [1]}], warmup [{"0", [2]}]
      <<1, 2, 2, "0", 2, 1::32, 0, 2, 2, "0", 2, 2::32, 0, 0>>,
      # is_classic false, member tag buffer
      <<0, 0>>,
      # authorized_operations
      <<-2_147_483_648::32-signed>>
    ]
  end

  @expected_topology %{
    epoch: 1,
    subtopologies: [
      %{
        subtopology_id: "0",
        source_topics: ["in"],
        repartition_sink_topics: [],
        state_changelog_topics: [
          %{
            name: "app-s-changelog",
            partitions: 2,
            replication_factor: 1,
            topic_configs: [%{key: "cleanup.policy", value: "compact", tagged_fields: []}],
            tagged_fields: []
          }
        ],
        repartition_source_topics: [
          %{
            name: "app-r",
            partitions: 4,
            replication_factor: 3,
            topic_configs: [],
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }
    ],
    tagged_fields: []
  }

  @expected_member %{
    member_id: "m1",
    member_epoch: 3,
    instance_id: nil,
    rack_id: "r1",
    client_id: "cid",
    client_host: "/127.0.0.1",
    topology_epoch: 1,
    process_id: "proc",
    user_endpoint: %{host: "localhost", port: 8080, tagged_fields: []},
    client_tags: [%{key: "zone", value: "a", tagged_fields: []}],
    task_offsets: [%{subtopology_id: "0", partition: 0, offset: 10, tagged_fields: []}],
    task_end_offsets: [%{subtopology_id: "0", partition: 0, offset: 12, tagged_fields: []}],
    assignment: %{
      active_tasks: [%{subtopology_id: "0", partitions: [0], tagged_fields: []}],
      standby_tasks: [],
      warmup_tasks: [],
      tagged_fields: []
    },
    target_assignment: %{
      active_tasks: [],
      standby_tasks: [%{subtopology_id: "0", partitions: [1], tagged_fields: []}],
      warmup_tasks: [%{subtopology_id: "0", partitions: [2], tagged_fields: []}],
      tagged_fields: []
    },
    is_classic: 0,
    tagged_fields: []
  }

  @expected_group %{
    error_code: 0,
    error_message: nil,
    group_id: "app",
    group_state: "stable",
    group_epoch: 3,
    assignment_epoch: 3,
    topology: @expected_topology,
    members: [@expected_member],
    authorized_operations: -2_147_483_648,
    tagged_fields: []
  }

  describe "v0 response" do
    alias StreamsGroupDescribe.V0.Response

    test "decodes a full group with no leftover bytes" do
      wire =
        IO.iodata_to_binary([
          # correlation_id + header tag buffer, throttle_time_ms
          <<8::32, 0, 0::32>>,
          # groups: 1 element
          <<2>>,
          group_head(),
          # group tag buffer, root tag buffer
          <<0, 0>>
        ])

      assert {response, <<>>} = Response.deserialize(wire)

      assert response == %Response{
               correlation_id: 8,
               throttle_time_ms: 0,
               groups: [@expected_group],
               tagged_fields: []
             }
    end

    test "decodes a not-found group (captured from apache/kafka:4.4.0)" do
      wire =
        IO.iodata_to_binary([
          <<8::32, 0, 0::32, 2>>,
          # GROUP_ID_NOT_FOUND (69) with a message
          <<69::16, 22, "Group nope not found.">>,
          # group_id, group_state "", epochs 0
          <<5, "nope", 1, 0::32, 0::32>>,
          # topology null, members [], authorized_operations default
          <<-1::8-signed, 1, -2_147_483_648::32-signed>>,
          <<0, 0>>
        ])

      assert {%Response{groups: [group]}, <<>>} = Response.deserialize(wire)
      assert group.error_code == 69
      assert group.error_message == "Group nope not found."
      assert group.topology == nil
      assert group.members == []
    end
  end

  describe "v1 response" do
    alias StreamsGroupDescribe.V1.Response

    test "group without a topology description" do
      wire =
        IO.iodata_to_binary([
          <<7::32, 0, 0::32, 2>>,
          group_head(),
          # topology_description: null, status NOT_STORED (1), assignor_name "sticky"
          <<-1::8-signed, 1, 7, "sticky">>,
          <<0, 0>>
        ])

      assert {response, <<>>} = Response.deserialize(wire)

      assert response == %Response{
               correlation_id: 7,
               throttle_time_ms: 0,
               groups: [
                 Map.merge(@expected_group, %{
                   topology_description: nil,
                   topology_description_status: 1,
                   assignor_name: "sticky"
                 })
               ],
               tagged_fields: []
             }
    end

    test "nested scopes: topology and topology_description subtopologies decode independently" do
      node = fn name, type, sources, sink, stores, successors ->
        [
          <<byte_size(name) + 1, name::binary, type::8>>,
          <<length(sources) + 1>>,
          Enum.map(sources, &<<byte_size(&1) + 1, &1::binary>>),
          if(sink, do: <<byte_size(sink) + 1, sink::binary>>, else: <<0>>),
          <<length(stores) + 1>>,
          Enum.map(stores, &<<byte_size(&1) + 1, &1::binary>>),
          <<length(successors) + 1>>,
          Enum.map(successors, &<<byte_size(&1) + 1, &1::binary>>),
          <<0>>
        ]
      end

      wire =
        IO.iodata_to_binary([
          <<7::32, 0, 0::32, 2>>,
          group_head(),
          # topology_description: present
          <<1>>,
          # subtopologies: [{"0", nodes: [source, sink]}]
          <<2, 2, "0", 3>>,
          node.("SRC", 1, ["in"], nil, [], ["SINK"]),
          node.("SINK", 3, [], "out", [], []),
          <<0>>,
          # global_stores: [{source, processor}]
          <<2>>,
          node.("GSRC", 1, ["global"], nil, [], ["GPROC"]),
          node.("GPROC", 2, [], nil, ["gstore"], []),
          <<0>>,
          # topology_description tag buffer
          <<0>>,
          # topology_description_status AVAILABLE (3), assignor_name null
          <<3, 0>>,
          <<0, 0>>
        ])

      assert {%Response{groups: [group]}, <<>>} = Response.deserialize(wire)

      # topology.subtopologies keeps its own fields...
      assert group.topology == @expected_topology

      # ...and topology_description.subtopologies has a different schema
      assert group.topology_description == %{
               subtopologies: [
                 %{
                   subtopology_id: "0",
                   nodes: [
                     %{
                       name: "SRC",
                       node_type: 1,
                       source_topics: ["in"],
                       sink_topic: nil,
                       stores: [],
                       successors: ["SINK"],
                       tagged_fields: []
                     },
                     %{
                       name: "SINK",
                       node_type: 3,
                       source_topics: [],
                       sink_topic: "out",
                       stores: [],
                       successors: [],
                       tagged_fields: []
                     }
                   ],
                   tagged_fields: []
                 }
               ],
               global_stores: [
                 %{
                   source: %{
                     name: "GSRC",
                     node_type: 1,
                     source_topics: ["global"],
                     sink_topic: nil,
                     stores: [],
                     successors: ["GPROC"],
                     tagged_fields: []
                   },
                   processor: %{
                     name: "GPROC",
                     node_type: 2,
                     source_topics: [],
                     sink_topic: nil,
                     stores: ["gstore"],
                     successors: [],
                     tagged_fields: []
                   },
                   tagged_fields: []
                 }
               ],
               tagged_fields: []
             }

      assert group.topology_description_status == 3
      assert group.assignor_name == nil
      # the member's assignment / target_assignment task lists stay distinct
      assert group.members == [@expected_member]
    end
  end
end
