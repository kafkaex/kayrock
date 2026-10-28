defmodule Kayrock.ShareAcknowledgeTest do
  @moduledoc """
  ShareAcknowledge (API key 79, KIP-932), v1-v2 (v0 was early access in Kafka
  4.0 and removed in 4.1). Always flexible.

    * v2 request: IsRenewAck (KIP-1222)
    * v2 response: AcquisitionLockTimeoutMs

  Fixtures are derived by hand from `ShareAcknowledge{Request,Response}.json`
  (Kafka 4.4.0); `share_acknowledge_v{1,2}_response.bin` were captured from an
  `apache/kafka:4.4.0` broker.
  """
  use ExUnit.Case, async: true

  alias Kayrock.ShareAcknowledge

  @topic_id <<0xFF::128>>
  @fixtures Path.expand("../../fixtures/share_groups", __DIR__)

  test "version range" do
    assert ShareAcknowledge.min_vsn()..ShareAcknowledge.max_vsn() ==
             Kayrock.TestSupport.api_version_range(:share_acknowledge)

    assert ShareAcknowledge.min_vsn() == 1
    assert ShareAcknowledge.max_vsn() == 2
    assert Kayrock.KafkaSchemaMetadata.api_key(:share_acknowledge) == 79
    assert %ShareAcknowledge.V1.Request{} = ShareAcknowledge.get_request_struct(1)
    assert %ShareAcknowledge.V2.Request{} = ShareAcknowledge.get_request_struct(2)
  end

  @topics [
    %{
      topic_id: @topic_id,
      partitions: [
        %{
          partition_index: 2,
          acknowledgement_batches: [
            %{first_offset: 0, last_offset: 1, acknowledge_types: [1]},
            %{first_offset: 2, last_offset: 2, acknowledge_types: [2]}
          ]
        }
      ]
    }
  ]

  @topics_bytes <<
    # topics: 1
    2,
    @topic_id::binary,
    # partitions: 1
    2,
    2::32,
    # acknowledgement_batches: 2
    3,
    0::64,
    1::64,
    # [ACCEPT]
    2,
    1,
    0,
    2::64,
    2::64,
    # [RELEASE]
    2,
    2,
    0,
    # partition tags, topic tags
    0,
    0
  >>

  test "v1 request" do
    request = %ShareAcknowledge.V1.Request{
      correlation_id: 4,
      client_id: "c",
      group_id: "g",
      member_id: "m",
      share_session_epoch: 2,
      topics: @topics
    }

    expected =
      <<
        # header v2
        79::16,
        1::16,
        4::32,
        1::16,
        "c",
        0,
        2,
        "g",
        2,
        "m",
        # share_session_epoch
        2::32
      >> <> @topics_bytes <> <<0>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v2 request adds is_renew_ack" do
    request = %ShareAcknowledge.V2.Request{
      correlation_id: 4,
      client_id: "c",
      group_id: nil,
      member_id: "m",
      share_session_epoch: 2,
      is_renew_ack: true,
      topics: @topics
    }

    expected =
      <<
        79::16,
        2::16,
        4::32,
        1::16,
        "c",
        0,
        # group_id null
        0,
        2,
        "m",
        2::32,
        # is_renew_ack
        1
      >> <> @topics_bytes <> <<0>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  @responses_bytes <<
    # responses: 1
    2,
    @topic_id::binary,
    # partitions: 1 {partition_index, error_code, error_message null}
    2,
    2::32,
    0::16,
    0,
    # current_leader
    -1::32-signed,
    -1::32-signed,
    0,
    # partition tags, topic tags
    0,
    0,
    # node_endpoints: 1 {node_id, host, port, rack "r", tags}
    2,
    1::32,
    2,
    "h",
    9092::32,
    2,
    "r",
    0,
    # tagged fields
    0
  >>

  @expected_responses [
    %{
      topic_id: @topic_id,
      partitions: [
        %{
          partition_index: 2,
          error_code: 0,
          error_message: nil,
          current_leader: %{leader_id: -1, leader_epoch: -1, tagged_fields: []},
          tagged_fields: []
        }
      ],
      tagged_fields: []
    }
  ]

  @expected_endpoints [%{node_id: 1, host: "h", port: 9092, rack: "r", tagged_fields: []}]

  test "v1 response (no acquisition_lock_timeout_ms)" do
    data = <<4::32, 0, 0::32, 0::16, 0>> <> @responses_bytes

    assert ShareAcknowledge.deserialize(1, data) ==
             {%ShareAcknowledge.V1.Response{
                correlation_id: 4,
                throttle_time_ms: 0,
                error_code: 0,
                error_message: nil,
                responses: @expected_responses,
                node_endpoints: @expected_endpoints,
                tagged_fields: []
              }, <<>>}
  end

  test "v2 response adds acquisition_lock_timeout_ms" do
    data = <<4::32, 0, 0::32, 0::16, 0, 15_000::32>> <> @responses_bytes

    assert ShareAcknowledge.deserialize(2, data) ==
             {%ShareAcknowledge.V2.Response{
                correlation_id: 4,
                throttle_time_ms: 0,
                error_code: 0,
                error_message: nil,
                acquisition_lock_timeout_ms: 15_000,
                responses: @expected_responses,
                node_endpoints: @expected_endpoints,
                tagged_fields: []
              }, <<>>}
  end

  test "top-level error response" do
    # SHARE_SESSION_NOT_FOUND (122), error_message "s", no responses
    data = <<4::32, 0, 0::32, 122::16, 2, "s", 1, 1, 0>>

    assert {%ShareAcknowledge.V1.Response{
              error_code: 122,
              error_message: "s",
              responses: [],
              node_endpoints: []
            }, <<>>} = ShareAcknowledge.deserialize(1, data)
  end

  for version <- 1..2 do
    test "v#{version} response captured from apache/kafka:4.4.0" do
      version = unquote(version)
      data = File.read!(Path.join(@fixtures, "share_acknowledge_v#{version}_response.bin"))

      assert {response, <<>>} = ShareAcknowledge.deserialize(version, data)
      assert response.error_code == 0

      assert [%{topic_id: <<_::128>>, partitions: [%{partition_index: 0, error_code: 0}]}] =
               response.responses

      if version == 2, do: assert(response.acquisition_lock_timeout_ms == 30_000)
    end
  end
end
