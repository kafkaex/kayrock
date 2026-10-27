defmodule Kayrock.ShareFetchTest do
  @moduledoc """
  ShareFetch (API key 78, KIP-932), v1-v2 (v0 was early access in Kafka 4.0
  and removed in 4.1). Always flexible.

    * v1: MaxRecords, BatchSize; `PartitionMaxBytes` (v0 only) is absent
    * v2: ShareAcquireMode and IsRenewAck (KIP-1206, KIP-1222); the response
      is unchanged

  Fixtures are derived by hand from `ShareFetch{Request,Response}.json`
  (Kafka 4.4.0); `share_fetch_v{1,2}_response.bin` were captured from an
  `apache/kafka:4.4.0` broker.
  """
  use ExUnit.Case, async: true

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.ShareFetch

  @topic_id <<0x0102030405060708090A0B0C0D0E0F10::128>>
  @fixtures Path.expand("../../fixtures/share_groups", __DIR__)

  test "version range" do
    assert ShareFetch.min_vsn()..ShareFetch.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:share_fetch)

    assert ShareFetch.min_vsn() == 1
    assert ShareFetch.max_vsn() == 2
    assert Kayrock.KafkaSchemaMetadata.api_key(:share_fetch) == 78
    assert %ShareFetch.V1.Request{} = ShareFetch.get_request_struct(1)
    assert %ShareFetch.V2.Request{} = ShareFetch.get_request_struct(2)
  end

  defp request_fields do
    %{
      correlation_id: 9,
      client_id: "c",
      group_id: "g",
      member_id: "m",
      share_session_epoch: 1,
      max_wait_ms: 500,
      min_bytes: 1,
      max_bytes: 1024,
      max_records: 10,
      batch_size: 5,
      topics: [
        %{
          topic_id: @topic_id,
          partitions: [
            %{
              partition_index: 0,
              acknowledgement_batches: [
                %{first_offset: 3, last_offset: 4, acknowledge_types: [1, 3]}
              ]
            }
          ]
        }
      ],
      forgotten_topics_data: [%{topic_id: @topic_id, partitions: [1, 2]}]
    }
  end

  # Everything after the v1 fixed fields (topics, forgotten topics, tags)
  @request_tail <<
    # topics: 1
    2,
    @topic_id::binary,
    # partitions: 1
    2,
    0::32,
    # acknowledgement_batches: 1
    2,
    3::64,
    4::64,
    # acknowledge_types [ACCEPT, REJECT]
    3,
    1,
    3,
    # batch tags, partition tags, topic tags
    0,
    0,
    0,
    # forgotten_topics_data: 1
    2,
    @topic_id::binary,
    # partitions [1, 2]
    3,
    1::32,
    2::32,
    0,
    # tagged fields
    0
  >>

  test "v1 request" do
    request = struct(ShareFetch.V1.Request, request_fields())

    expected =
      <<
        # header v2
        78::16,
        1::16,
        9::32,
        1::16,
        "c",
        0,
        # group_id, member_id (compact nullable strings)
        2,
        "g",
        2,
        "m",
        # share_session_epoch, max_wait_ms, min_bytes, max_bytes
        1::32,
        500::32,
        1::32,
        1024::32,
        # max_records, batch_size
        10::32,
        5::32
      >> <> @request_tail

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v2 request adds share_acquire_mode and is_renew_ack" do
    request =
      struct(
        ShareFetch.V2.Request,
        Map.merge(request_fields(), %{share_acquire_mode: 1, is_renew_ack: true})
      )

    expected =
      <<
        78::16,
        2::16,
        9::32,
        1::16,
        "c",
        0,
        2,
        "g",
        2,
        "m",
        1::32,
        500::32,
        1::32,
        1024::32,
        10::32,
        5::32,
        # share_acquire_mode (record-limit), is_renew_ack
        1,
        1
      >> <> @request_tail

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "close-session request: null group, epoch -1, no topics" do
    request = %ShareFetch.V2.Request{
      correlation_id: 1,
      client_id: "c",
      group_id: nil,
      member_id: "m",
      share_session_epoch: -1,
      max_wait_ms: 0,
      min_bytes: 0,
      max_bytes: 0,
      max_records: 0,
      batch_size: 0,
      share_acquire_mode: 0,
      is_renew_ack: false,
      topics: [],
      forgotten_topics_data: []
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<78::16, 2::16, 1::32, 1::16, "c", 0, 0, 2, "m", -1::32-signed, 0::32, 0::32, 0::32,
               0::32, 0::32, 0, 0, 1, 1, 0>>
  end

  defp record_batch do
    %RecordBatch{
      batch_offset: 3,
      records: [%Record{offset: 3, key: "k", value: "v", timestamp: 1}]
    }
  end

  defp response_binary(batch_bytes) do
    <<
      # correlation_id, header tagged fields
      9::32,
      0,
      # throttle_time_ms, error_code, error_message null
      0::32,
      0::16,
      0,
      # acquisition_lock_timeout_ms
      30_000::32,
      # responses: 1
      2,
      @topic_id::binary,
      # partitions: 1
      2,
      0::32,
      # error_code, error_message null
      0::16,
      0,
      # acknowledge_error_code, acknowledge_error_message null
      0::16,
      0,
      # current_leader {leader_id, leader_epoch, tags}
      1::32,
      0::32,
      0,
      # records (compact records: varint length + 1)
      byte_size(batch_bytes) + 1,
      batch_bytes::binary,
      # acquired_records: 1
      2,
      3::64,
      3::64,
      1::16,
      0,
      # partition tags, topic tags
      0,
      0,
      # node_endpoints: 1 {node_id, host, port, rack null, tags}
      2,
      1::32,
      3,
      "h1",
      9092::32,
      0,
      0,
      # tagged fields
      0
    >>
  end

  for version <- 1..2 do
    test "v#{version} response with records" do
      version = unquote(version)

      <<_size::32, batch_bytes::binary>> =
        IO.iodata_to_binary(RecordBatch.serialize(record_batch()))

      assert byte_size(batch_bytes) < 127
      data = response_binary(batch_bytes)

      assert {response, <<>>} = ShareFetch.deserialize(version, data)
      assert response.__struct__ == Module.concat([ShareFetch, "V#{version}", Response])
      assert response.correlation_id == 9
      assert response.acquisition_lock_timeout_ms == 30_000

      assert response.node_endpoints == [
               %{node_id: 1, host: "h1", port: 9092, rack: nil, tagged_fields: []}
             ]

      assert [%{topic_id: @topic_id, partitions: [partition], tagged_fields: []}] =
               response.responses

      assert partition.partition_index == 0
      assert partition.error_code == 0
      assert partition.acknowledge_error_code == 0
      assert partition.current_leader == %{leader_id: 1, leader_epoch: 0, tagged_fields: []}

      assert partition.acquired_records == [
               %{first_offset: 3, last_offset: 3, delivery_count: 1, tagged_fields: []}
             ]

      assert [%RecordBatch{batch_offset: 3, records: [record]}] = partition.records
      assert %Record{offset: 3, key: "k", value: "v"} = record
    end
  end

  test "response with null records and an acknowledge error" do
    data = <<
      9::32,
      0,
      0::32,
      0::16,
      0,
      30_000::32,
      2,
      @topic_id::binary,
      2,
      0::32,
      # NOT_LEADER_OR_FOLLOWER, error_message "e"
      6::16,
      2,
      "e",
      # INVALID_RECORD_STATE, acknowledge_error_message "a"
      121::16,
      2,
      "a",
      2::32,
      5::32,
      0,
      # records null
      0,
      # acquired_records []
      1,
      0,
      0,
      # node_endpoints []
      1,
      0
    >>

    assert {%ShareFetch.V2.Response{responses: [%{partitions: [partition]}], node_endpoints: []},
            <<>>} = ShareFetch.V2.Response.deserialize(data)

    assert %{
             error_code: 6,
             error_message: "e",
             acknowledge_error_code: 121,
             acknowledge_error_message: "a",
             current_leader: %{leader_id: 2, leader_epoch: 5},
             records: nil,
             acquired_records: []
           } = partition
  end

  for version <- 1..2 do
    test "v#{version} response captured from apache/kafka:4.4.0" do
      version = unquote(version)
      data = File.read!(Path.join(@fixtures, "share_fetch_v#{version}_response.bin"))

      assert {response, <<>>} = ShareFetch.deserialize(version, data)
      assert response.error_code == 0
      assert response.acquisition_lock_timeout_ms == 30_000
      assert [%{topic_id: <<_::128>>, partitions: [partition]}] = response.responses
      assert partition.error_code == 0

      assert [%{first_offset: first, last_offset: last, delivery_count: 1}] =
               partition.acquired_records

      offsets = for batch <- partition.records, r <- batch.records, do: r.offset
      assert offsets == Enum.to_list(first..last)
      assert Enum.all?(partition.records, &match?(%RecordBatch{}, &1))
    end
  end
end
