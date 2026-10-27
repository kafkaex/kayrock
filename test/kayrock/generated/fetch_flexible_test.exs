defmodule Kayrock.Generated.FetchFlexibleTest do
  @moduledoc """
  Fetch v12-v18 (JSON-derived, flexible): byte-level request fixtures and
  response fixtures hand-derived from `FetchRequest.json` /
  `FetchResponse.json` (Kafka 4.4.0).

    * v12: flexible; `ClusterId` request tag; `DivergingEpoch` /
      `CurrentLeader` / `SnapshotId` partition response tags; records are
      `compact_records`
    * v13: topic IDs instead of topic names (requests and responses)
    * v15: `ReplicaId` replaced by the `ReplicaState` request tag
    * v16: `NodeEndpoints` response tag
    * v17: `ReplicaDirectoryId` partition request tag
    * v18: `HighWatermark` partition request tag

  `forgotten_topics_data[].partitions` is `[]int32` (a compact array of
  int32). The kpro-sourced v7-v11 model it as an array of structs; that quirk
  is kept for those versions and is not carried into v12+.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Test.RecordBatchFixture

  @topic_id <<0xA1, 0xA2, 0xA3, 0xA4, 0xB1, 0xB2, 0xC1, 0xC2, 0xD1, 0xD2, 0xE1, 0xE2, 0xE3, 0xE4,
              0xE5, 0xE6>>

  @dir_id <<1::128>>

  defp request_module(vsn), do: Module.concat([Kayrock.Fetch, :"V#{vsn}", Request])
  defp response_module(vsn), do: Module.concat([Kayrock.Fetch, :"V#{vsn}", Response])

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  defp request_header(vsn), do: <<1::16, vsn::16, 7::32, 2::16, "kr", 0>>

  defp topic_key(vsn) when vsn >= 13, do: %{topic_id: @topic_id}
  defp topic_key(_vsn), do: %{topic: "t"}

  defp topic_key_bytes(vsn) when vsn >= 13, do: @topic_id
  defp topic_key_bytes(_vsn), do: <<2, "t">>

  defp base_request(vsn) do
    fields = [
      correlation_id: 7,
      client_id: "kr",
      max_wait_ms: 500,
      min_bytes: 1,
      max_bytes: 1_048_576,
      isolation_level: 1,
      session_id: 0,
      session_epoch: -1,
      topics: [
        Map.put(topic_key(vsn), :partitions, [
          %{
            partition: 3,
            current_leader_epoch: -1,
            fetch_offset: 42,
            last_fetched_epoch: -1,
            log_start_offset: -1,
            partition_max_bytes: 65_536
          }
        ])
      ],
      forgotten_topics_data: [Map.put(topic_key(vsn), :partitions, [0, 5])],
      rack_id: "r1"
    ]

    fields = if vsn < 15, do: [{:replica_id, -1} | fields], else: fields
    struct(request_module(vsn), fields)
  end

  # Request bytes after the header and before the root tag section
  defp base_request_body(vsn) do
    IO.iodata_to_binary([
      if(vsn < 15, do: <<-1::32-signed>>, else: <<>>),
      # max_wait_ms, min_bytes, max_bytes, isolation_level, session_id,
      # session_epoch
      <<500::32, 1::32, 1_048_576::32, 1, 0::32, -1::32-signed>>,
      # topics: 1
      <<2>>,
      topic_key_bytes(vsn),
      # partitions: 1 {partition 3, current_leader_epoch -1, fetch_offset 42,
      # last_fetched_epoch -1, log_start_offset -1, partition_max_bytes 65536}
      <<2, 3::32, -1::32-signed, 42::64, -1::32-signed, -1::64-signed, 65_536::32>>,
      # partition and topic tag buffers (all named tags at their defaults)
      <<0, 0>>,
      # forgotten_topics_data: 1 {topic key, partitions: compact []int32 [0, 5]}
      <<2>>,
      topic_key_bytes(vsn),
      <<3, 0::32, 5::32, 0>>,
      # rack_id "r1"
      <<3, "r1">>
    ])
  end

  test "the generated range now reaches v18" do
    assert Kayrock.Fetch.max_vsn() == 18
    assert Kayrock.KafkaSchemaMetadata.version_range(:fetch) == {0, 18}
  end

  test "v12+ model forgotten_topics_data partitions as []int32, unlike kpro-sourced v7-v11" do
    for vsn <- 12..18 do
      {:compact_array, forgotten} =
        Keyword.fetch!(request_module(vsn).schema(), :forgotten_topics_data)

      assert Keyword.fetch!(forgotten, :partitions) == {:compact_array, :int32}, "v#{vsn}"
    end

    {:array, forgotten} =
      Keyword.fetch!(Kayrock.Fetch.V11.Request.schema(), :forgotten_topics_data)

    # kpro_schema quirk: an array of partition structs
    assert {:array, [{:partition, :int32} | _]} = Keyword.fetch!(forgotten, :partitions)
  end

  describe "requests" do
    for vsn <- 12..18 do
      test "v#{vsn} serializes with every tagged field at its default" do
        vsn = unquote(vsn)

        assert serialize(base_request(vsn)) ==
                 request_header(vsn) <> base_request_body(vsn) <> <<0>>
      end
    end

    test "v12 writes a non-null ClusterId as root tag 0" do
      request = %{base_request(12) | cluster_id: "c1"}

      assert serialize(request) ==
               request_header(12) <> base_request_body(12) <> <<1, 0, 3, 3, "c1">>
    end

    test "v15 writes ReplicaState as root tag 1, after ClusterId" do
      request = %{
        base_request(15)
        | cluster_id: "c",
          replica_state: %{replica_id: 2, replica_epoch: 9}
      }

      assert serialize(request) ==
               IO.iodata_to_binary([
                 request_header(15),
                 base_request_body(15),
                 # 2 tags: {0, size 2, "c"}, {1, size 13, replica_id 2,
                 # replica_epoch 9, empty tag buffer}
                 <<2, 0, 2, 2, "c", 1, 13, 2::32, 9::64, 0>>
               ])
    end

    test "v18 writes ReplicaDirectoryId (tag 0) and HighWatermark (tag 1) partition tags" do
      request = base_request(18)
      [topic] = request.topics
      [partition] = topic.partitions
      partition = Map.merge(partition, %{replica_directory_id: @dir_id, high_watermark: 77})
      request = %{request | topics: [%{topic | partitions: [partition]}]}

      assert serialize(request) ==
               IO.iodata_to_binary([
                 request_header(18),
                 <<500::32, 1::32, 1_048_576::32, 1, 0::32, -1::32-signed>>,
                 <<2>>,
                 @topic_id,
                 <<2, 3::32, -1::32-signed, 42::64, -1::32-signed, -1::64-signed, 65_536::32>>,
                 # partition tags: {0, size 16, dir id}, {1, size 8, 77}
                 <<2, 0, 16>>,
                 @dir_id,
                 <<1, 8, 77::64>>,
                 # topic tags; forgotten topics; rack; root tags
                 <<0, 2>>,
                 @topic_id,
                 <<3, 0::32, 5::32, 0, 3, "r1", 0>>
               ])
    end

    test "v17 omits ReplicaDirectoryId when it is the zero UUID default" do
      request = base_request(17)
      [topic] = request.topics
      [partition] = topic.partitions
      partition = Map.put(partition, :replica_directory_id, <<0::128>>)
      request = %{request | topics: [%{topic | partitions: [partition]}]}

      assert serialize(request) == request_header(17) <> base_request_body(17) <> <<0>>
    end
  end

  describe "responses" do
    # Response bytes for one partition with a record batch and one aborted
    # transaction; `partition_tags` is the partition's tag section.
    defp response_wire(vsn, partition_tags, root_tags) do
      IO.iodata_to_binary([
        # response header v1
        <<7::32, 0>>,
        # throttle_time_ms 0, error_code 0, session_id 12
        <<0::32, 0::16, 12::32>>,
        # responses: 1 {topic key, partitions: 1}
        <<2>>,
        topic_key_bytes(vsn),
        <<2>>,
        # partition_index 3, error_code 0, high_watermark 1, last_stable_offset 1,
        # log_start_offset 0
        <<3::32, 0::16, 1::64, 1::64, 0::64>>,
        # aborted_transactions: 1 {producer_id 5, first_offset 0, tags}
        <<2, 5::64, 0::64, 0>>,
        # preferred_read_replica -1
        <<-1::32-signed>>,
        # records: compact_records, varint(74 + 1)
        <<75>>,
        RecordBatchFixture.binary(),
        partition_tags,
        # topic tag buffer
        <<0>>,
        root_tags
      ])
    end

    for vsn <- 12..18 do
      test "v#{vsn} decodes compact records and partition fields" do
        vsn = unquote(vsn)

        assert {response, <<>>} =
                 response_module(vsn).deserialize(response_wire(vsn, <<0>>, <<0>>))

        assert response.correlation_id == 7
        assert response.throttle_time_ms == 0
        assert response.error_code == 0
        assert response.session_id == 12
        assert response.tagged_fields == []

        [topic] = response.responses
        assert Map.take(topic, [:topic, :topic_id]) == topic_key(vsn)
        assert topic.tagged_fields == []

        assert topic.partitions == [
                 %{
                   partition_index: 3,
                   error_code: 0,
                   high_watermark: 1,
                   last_stable_offset: 1,
                   log_start_offset: 0,
                   aborted_transactions: [%{producer_id: 5, first_offset: 0, tagged_fields: []}],
                   preferred_read_replica: -1,
                   records: RecordBatchFixture.decoded(),
                   diverging_epoch: nil,
                   current_leader: nil,
                   snapshot_id: nil,
                   tagged_fields: []
                 }
               ]

        if vsn >= 16, do: assert(response.node_endpoints == [])
      end
    end

    test "v12 decodes null aborted_transactions and null records" do
      wire =
        IO.iodata_to_binary([
          <<7::32, 0, 0::32, 0::16, 0::32, 2, 2, "t", 2>>,
          <<0::32, 1::16, -1::64-signed, -1::64-signed, -1::64-signed>>,
          # aborted_transactions: null, preferred_read_replica -1, records: null
          <<0, -1::32-signed, 0>>,
          <<0, 0, 0>>
        ])

      assert {response, <<>>} = response_module(12).deserialize(wire)
      [%{partitions: [partition]}] = response.responses
      assert partition.error_code == 1
      assert partition.aborted_transactions == nil
      assert partition.records == nil
    end

    test "v16 decodes all three partition tags and NodeEndpoints" do
      partition_tags =
        IO.iodata_to_binary([
          <<3>>,
          # tag 0 DivergingEpoch {epoch 4, end_offset 100, tags}: size 13
          <<0, 13, 4::32, 100::64, 0>>,
          # tag 1 CurrentLeader {leader_id 1, leader_epoch 5, tags}: size 9
          <<1, 9, 1::32, 5::32, 0>>,
          # tag 2 SnapshotId {end_offset 50, epoch 3, tags}: size 13
          <<2, 13, 50::64, 3::32, 0>>
        ])

      # NodeEndpoints: 1 {node_id 1, host "h", port 9092, rack "r", tags}: size 14
      root_tags = <<1, 0, 14, 2, 1::32, 2, "h", 9092::32, 2, "r", 0>>

      assert {response, <<>>} =
               response_module(16).deserialize(response_wire(16, partition_tags, root_tags))

      [%{partitions: [partition]}] = response.responses
      assert partition.diverging_epoch == %{epoch: 4, end_offset: 100, tagged_fields: []}
      assert partition.current_leader == %{leader_id: 1, leader_epoch: 5, tagged_fields: []}
      assert partition.snapshot_id == %{end_offset: 50, epoch: 3, tagged_fields: []}
      assert partition.tagged_fields == []

      assert response.node_endpoints == [
               %{node_id: 1, host: "h", port: 9092, rack: "r", tagged_fields: []}
             ]
    end

    test "v18 decodes a record batch that round-trips back to the same records" do
      {response, <<>>} = response_module(18).deserialize(response_wire(18, <<0>>, <<0>>))
      [%{partitions: [%{records: [batch]}]}] = response.responses

      request =
        struct(Kayrock.Produce.V13.Request,
          correlation_id: 1,
          client_id: nil,
          transactional_id: nil,
          acks: 1,
          timeout_ms: 1,
          topic_data: [%{topic_id: @topic_id, partition_data: [%{index: 3, records: batch}]}]
        )

      # The decoded batch re-encodes to the exact fixture bytes
      assert :binary.match(serialize(request), <<75, RecordBatchFixture.binary()::binary>>) !=
               :nomatch
    end
  end
end
