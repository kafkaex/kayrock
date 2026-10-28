defmodule Kayrock.StreamsGroupHeartbeatTest do
  @moduledoc """
  Tests for StreamsGroupHeartbeat (API key 88, KIP-1071), v0-v1.

  Generated from the Apache Kafka 4.4.0 JSON spec. Both versions are
  flexible. The requests are identical; the v1 response drops
  `acceptable_recovery_lag_legacy` (int32) and adds `acceptable_recovery_lag`
  (int64) and `topology_description_required` (bool, KIP-1331).

  Wire fixtures are derived by hand from
  `priv/kafka_messages/4.4.0/StreamsGroupHeartbeat{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  alias Kayrock.StreamsGroupHeartbeat

  test "version metadata" do
    assert StreamsGroupHeartbeat.min_vsn() == 0
    assert StreamsGroupHeartbeat.max_vsn() == 1
    assert Kayrock.KafkaSchemaMetadata.api_key(:streams_group_heartbeat) == 88
    assert Kayrock.KafkaSchemaMetadata.version_range(:streams_group_heartbeat) == {0, 1}

    for vsn <- 0..1 do
      request = StreamsGroupHeartbeat.get_request_struct(vsn)
      assert request.__struct__.api_key() == 88
      assert request.__struct__.api_vsn() == vsn
      # nullable struct fields default to null, arrays to empty
      assert request.topology == nil
      assert request.user_endpoint == nil
      assert request.active_tasks == []
    end
  end

  # A joining member (member_epoch 0) sending its full topology.
  defp join_request(module) do
    struct(module,
      correlation_id: 4,
      client_id: "k",
      group_id: "g",
      member_id: "m1",
      member_epoch: 0,
      endpoint_information_epoch: 0,
      instance_id: nil,
      rack_id: "r",
      rebalance_timeout_ms: 30_000,
      topology: %{
        epoch: 2,
        subtopologies: [
          %{
            subtopology_id: "0",
            source_topics: ["in"],
            source_topic_regex: [],
            state_changelog_topics: [
              %{
                name: "app-store-changelog",
                partitions: 0,
                replication_factor: 0,
                topic_configs: [%{key: "cleanup.policy", value: "compact"}]
              }
            ],
            repartition_sink_topics: [],
            repartition_source_topics: [],
            copartition_groups: [
              %{source_topics: [0, 1], source_topic_regex: [], repartition_source_topics: [2]}
            ]
          }
        ]
      },
      active_tasks: [],
      standby_tasks: [],
      warmup_tasks: [],
      process_id: "p",
      user_endpoint: %{host: "h", port: 65_535},
      client_tags: [%{key: "zone", value: "a"}],
      task_offsets: [%{subtopology_id: "0", partition: 1, offset: 42}],
      task_end_offsets: [],
      shutdown_application: false
    )
  end

  @join_body IO.iodata_to_binary([
               # group_id "g", member_id "m1"
               <<2, "g", 3, "m1">>,
               # member_epoch, endpoint_information_epoch
               <<0::32, 0::32>>,
               # instance_id: null, rack_id "r"
               <<0, 2, "r">>,
               # rebalance_timeout_ms
               <<30_000::32>>,
               # topology: present, epoch 2
               <<1, 2::32>>,
               # subtopologies: 1 element; subtopology_id "0"
               <<2, 2, "0">>,
               # source_topics ["in"], source_topic_regex []
               <<2, 3, "in", 1>>,
               # state_changelog_topics: 1 element
               <<2, 20, "app-store-changelog", 0::32, 0::16>>,
               # topic_configs: [{"cleanup.policy", "compact"}] + element tag buffer
               <<2, 15, "cleanup.policy", 8, "compact", 0>>,
               # state_changelog_topics element tag buffer
               <<0>>,
               # repartition_sink_topics [], repartition_source_topics []
               <<1, 1>>,
               # copartition_groups: 1 element of compact int16 arrays
               <<2, 3, 0::16, 1::16, 1, 2, 2::16, 0>>,
               # subtopology tag buffer, topology tag buffer
               <<0, 0>>,
               # active_tasks [], standby_tasks [], warmup_tasks []
               <<1, 1, 1>>,
               # process_id "p"
               <<2, "p">>,
               # user_endpoint: present, host "h", port 65535 (uint16), tag buffer
               <<1, 2, "h", 65_535::16, 0>>,
               # client_tags [{"zone", "a"}]
               <<2, 5, "zone", 2, "a", 0>>,
               # task_offsets [{"0", 1, 42}]
               <<2, 2, "0", 1::32, 42::64, 0>>,
               # task_end_offsets []
               <<1>>,
               # shutdown_application: false, tag buffer
               <<0, 0>>
             ])

  # A steady-state heartbeat: every "unchanged since last heartbeat" field
  # null. Generated structs default (compact) arrays to `[]`, so the nullable
  # arrays have to be set to nil explicitly.
  defp steady_request(module) do
    struct(module,
      correlation_id: 5,
      client_id: "k",
      group_id: "g",
      member_id: "m1",
      member_epoch: 3,
      endpoint_information_epoch: 1,
      rebalance_timeout_ms: -1,
      active_tasks: nil,
      standby_tasks: nil,
      warmup_tasks: nil,
      client_tags: nil,
      task_offsets: nil,
      task_end_offsets: nil,
      shutdown_application: true
    )
  end

  @steady_body IO.iodata_to_binary([
                 <<2, "g", 3, "m1">>,
                 <<3::32, 1::32>>,
                 # instance_id, rack_id: null
                 <<0, 0>>,
                 <<-1::32-signed>>,
                 # topology: null (nullable struct marker -1)
                 <<-1::8-signed>>,
                 # active/standby/warmup tasks: null compact arrays
                 <<0, 0, 0>>,
                 # process_id: null
                 <<0>>,
                 # user_endpoint: null
                 <<-1::8-signed>>,
                 # client_tags, task_offsets, task_end_offsets: null
                 <<0, 0, 0>>,
                 # shutdown_application: true, tag buffer
                 <<1, 0>>
               ])

  for vsn <- 0..1 do
    describe "v#{vsn} request" do
      test "joining member with a full topology" do
        vsn = unquote(vsn)
        request = join_request(StreamsGroupHeartbeat.get_request_struct(vsn).__struct__)

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
                 <<88::16, vsn::16, 4::32, 1::16, "k", 0>> <> @join_body
      end

      test "steady-state heartbeat with every optional field null" do
        vsn = unquote(vsn)
        request = steady_request(StreamsGroupHeartbeat.get_request_struct(vsn).__struct__)

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
                 <<88::16, vsn::16, 5::32, 1::16, "k", 0>> <> @steady_body
      end
    end
  end

  describe "v0 response" do
    alias Kayrock.StreamsGroupHeartbeat.V0.Response

    # Captured from apache/kafka:4.4.0 for a joining member while assignment
    # was delayed by group.streams.initial.rebalance.delay.ms.
    @v0_wire IO.iodata_to_binary([
               # correlation_id + header tag buffer
               <<10::32, 0>>,
               # throttle_time_ms, error_code, error_message: null
               <<0::32, 0::16, 0>>,
               # member_id "member-2", member_epoch, heartbeat_interval_ms
               <<9, "member-2", 1::32, 5000::32>>,
               # acceptable_recovery_lag_legacy, task_offset_interval_ms
               <<0::32, 60_000::32>>,
               # status: [{5 ASSIGNMENT_DELAYED, detail}]
               <<2, 5, 66, "Assignment delayed due to the configured initial rebalance delay.",
                 0>>,
               # active_tasks [], standby_tasks [], warmup_tasks []
               <<1, 1, 1>>,
               # endpoint_information_epoch
               <<0::32>>,
               # partitions_by_user_endpoint: one entry, endpoint localhost:8080
               <<2, 10, "localhost", 8080::16, 0>>,
               # active_partitions [], standby_partitions [], element tag buffer
               <<1, 1, 0>>,
               # tag buffer
               <<0>>
             ])

    test "decodes every field with no leftover bytes" do
      assert {response, <<>>} = Response.deserialize(@v0_wire)

      assert response == %Response{
               correlation_id: 10,
               throttle_time_ms: 0,
               error_code: 0,
               error_message: nil,
               member_id: "member-2",
               member_epoch: 1,
               heartbeat_interval_ms: 5000,
               acceptable_recovery_lag_legacy: 0,
               task_offset_interval_ms: 60_000,
               status: [
                 %{
                   status_code: 5,
                   status_detail:
                     "Assignment delayed due to the configured initial rebalance delay.",
                   tagged_fields: []
                 }
               ],
               active_tasks: [],
               standby_tasks: [],
               warmup_tasks: [],
               endpoint_information_epoch: 0,
               partitions_by_user_endpoint: [
                 %{
                   user_endpoint: %{host: "localhost", port: 8080, tagged_fields: []},
                   active_partitions: [],
                   standby_partitions: [],
                   tagged_fields: []
                 }
               ],
               tagged_fields: []
             }

      refute Map.has_key?(response, :acceptable_recovery_lag)
      refute Map.has_key?(response, :topology_description_required)
    end
  end

  describe "v1 response" do
    alias Kayrock.StreamsGroupHeartbeat.V1.Response

    test "decodes an assignment with v1 fields and no leftover bytes" do
      wire =
        IO.iodata_to_binary([
          <<11::32, 0>>,
          <<0::32, 0::16, 0>>,
          <<3, "m1", 2::32, 5000::32>>,
          # task_offset_interval_ms, acceptable_recovery_lag (int64)
          <<60_000::32, 10_000::64>>,
          # status: null
          <<0>>,
          # active_tasks [{"0", [0, 1]}]
          <<2, 2, "0", 3, 0::32, 1::32, 0>>,
          # standby_tasks: [], warmup_tasks: null
          <<1, 0>>,
          # topology_description_required: true
          <<1>>,
          # endpoint_information_epoch
          <<3::32>>,
          # partitions_by_user_endpoint: [{h:9000, active [{"in", [0, 1]}], standby []}]
          <<2, 2, "h", 9000::16, 0>>,
          <<2, 3, "in", 3, 0::32, 1::32, 0>>,
          <<1, 0>>,
          <<0>>
        ])

      assert {response, <<>>} = StreamsGroupHeartbeat.deserialize(1, wire)

      assert %Response{
               correlation_id: 11,
               member_id: "m1",
               member_epoch: 2,
               task_offset_interval_ms: 60_000,
               acceptable_recovery_lag: 10_000,
               status: nil,
               active_tasks: [%{subtopology_id: "0", partitions: [0, 1], tagged_fields: []}],
               standby_tasks: [],
               warmup_tasks: nil,
               topology_description_required: 1,
               endpoint_information_epoch: 3,
               partitions_by_user_endpoint: [
                 %{
                   user_endpoint: %{host: "h", port: 9000},
                   active_partitions: [%{topic: "in", partitions: [0, 1]}],
                   standby_partitions: []
                 }
               ]
             } = response

      refute Map.has_key?(response, :acceptable_recovery_lag_legacy)
    end

    test "decodes a top-level error" do
      # GROUP_ID_NOT_FOUND (69)
      wire =
        IO.iodata_to_binary([
          <<5::32, 0>>,
          <<0::32, 69::16, 9, "no group">>,
          # member_id "", member_epoch, heartbeat_interval_ms
          <<1, 0::32, 0::32>>,
          # task_offset_interval_ms, acceptable_recovery_lag -1
          <<0::32, -1::64-signed>>,
          # status [], active/standby/warmup tasks null
          <<1, 0, 0, 0>>,
          # topology_description_required false, endpoint_information_epoch
          <<0, 0::32>>,
          # partitions_by_user_endpoint null, tag buffer
          <<0, 0>>
        ])

      assert {%Response{
                error_code: 69,
                error_message: "no group",
                member_id: "",
                acceptable_recovery_lag: -1,
                status: [],
                active_tasks: nil,
                partitions_by_user_endpoint: nil
              }, <<>>} = Response.deserialize(wire)
    end
  end
end
