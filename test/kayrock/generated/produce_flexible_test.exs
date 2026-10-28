defmodule Kayrock.Generated.ProduceFlexibleTest do
  @moduledoc """
  Produce v9-v13 (JSON-derived, flexible): byte-level request fixtures and
  response fixtures hand-derived from `ProduceRequest.json` /
  `ProduceResponse.json` (Kafka 4.4.0).

  v9 enables flexible versions (compact encodings, tagged fields,
  `compact_records`), v10 adds the `CurrentLeader` / `NodeEndpoints` tagged
  fields, v11-v12 are wire-identical to v10, and v13 replaces topic names with
  topic IDs (KIP-516).
  """
  use ExUnit.Case, async: true

  alias Kayrock.Test.RecordBatchFixture

  @topic_id <<0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xAA, 0xBB, 0xCC, 0xDD, 0xEE,
              0xFF, 0x00>>

  defp request_module(vsn), do: Module.concat([Kayrock.Produce, :"V#{vsn}", Request])
  defp response_module(vsn), do: Module.concat([Kayrock.Produce, :"V#{vsn}", Response])

  defp serialize(request), do: IO.iodata_to_binary(Kayrock.Request.serialize(request))

  # request header v2: api_key 0, version, correlation_id 7, client_id "kr",
  # empty header tag buffer
  defp request_header(vsn), do: <<0::16, vsn::16, 7::32, 2::16, "kr", 0>>

  defp topic_key(13), do: %{topic_id: @topic_id}
  defp topic_key(_vsn), do: %{name: "t"}

  defp topic_key_bytes(13), do: @topic_id
  defp topic_key_bytes(_vsn), do: <<2, "t">>

  test "the fixture record batch has a valid CRC-32C" do
    assert RecordBatchFixture.crc_valid?()
    assert byte_size(RecordBatchFixture.binary()) == 74
  end

  test "the generated range now reaches v13" do
    assert Kayrock.Produce.max_vsn() == 13
    assert Kayrock.KafkaSchemaMetadata.version_range(:produce) == {0, 13}
  end

  describe "requests" do
    for vsn <- 9..13 do
      test "v#{vsn} serializes a record batch through compact_records" do
        vsn = unquote(vsn)

        request =
          struct(request_module(vsn),
            correlation_id: 7,
            client_id: "kr",
            transactional_id: nil,
            acks: -1,
            timeout_ms: 1500,
            topic_data: [
              Map.put(topic_key(vsn), :partition_data, [
                %{index: 2, records: RecordBatchFixture.request_batch()}
              ])
            ]
          )

        expected =
          IO.iodata_to_binary([
            request_header(vsn),
            # transactional_id: null (compact, 0), acks -1, timeout_ms
            <<0, -1::16-signed, 1500::32>>,
            # topic_data: compact array of 1 (varint 2)
            <<2>>,
            topic_key_bytes(vsn),
            # partition_data: 1 {index 2, records: varint(74 + 1) = 75, batch}
            <<2, 2::32, 75>>,
            RecordBatchFixture.binary(),
            # partition, topic and root tag buffers
            <<0, 0, 0>>
          ])

        assert serialize(request) == expected
      end
    end

    test "v9 null records serialize as varint 0 and a transactional id as a compact string" do
      request =
        struct(request_module(9),
          correlation_id: 7,
          client_id: "kr",
          transactional_id: "tx",
          acks: 1,
          timeout_ms: 10,
          topic_data: [%{name: "t", partition_data: [%{index: 0, records: nil}]}]
        )

      assert serialize(request) ==
               request_header(9) <>
                 <<3, "tx", 1::16, 10::32, 2, 2, "t", 2, 0::32, 0, 0, 0, 0>>
    end
  end

  describe "responses" do
    test "v9 decodes (no named tagged fields yet)" do
      wire =
        IO.iodata_to_binary([
          # response header v1: correlation_id 7, empty tag buffer
          <<7::32, 0>>,
          # responses: 1 {name "t", partition_responses: 1 {...}}
          <<2, 2, "t", 2>>,
          # index 2, error_code 0, base_offset 41, log_append_time_ms -1,
          # log_start_offset 0
          <<2::32, 0::16, 41::64, -1::64-signed, 0::64>>,
          # record_errors: empty, error_message: null, partition tag buffer
          <<1, 0, 0>>,
          # topic tag buffer, throttle_time_ms 0, root tag buffer
          <<0, 0::32, 0>>
        ])

      assert {response, <<>>} = response_module(9).deserialize(wire)
      assert response.correlation_id == 7
      assert response.throttle_time_ms == 0

      assert response.responses == [
               %{
                 name: "t",
                 partition_responses: [
                   %{
                     index: 2,
                     error_code: 0,
                     base_offset: 41,
                     log_append_time_ms: -1,
                     log_start_offset: 0,
                     record_errors: [],
                     error_message: nil,
                     tagged_fields: []
                   }
                 ],
                 tagged_fields: []
               }
             ]

      assert response.tagged_fields == []
    end

    for vsn <- 10..13 do
      test "v#{vsn} decodes record errors and the CurrentLeader / NodeEndpoints tagged fields" do
        vsn = unquote(vsn)

        wire =
          IO.iodata_to_binary([
            <<7::32, 0>>,
            <<2>>,
            topic_key_bytes(vsn),
            # partition_responses: 1
            <<2>>,
            # index 0, error_code 10 (MESSAGE_TOO_LARGE), base_offset -1,
            # log_append_time_ms -1, log_start_offset -1
            <<0::32, 10::16, -1::64-signed, -1::64-signed, -1::64-signed>>,
            # record_errors: 1 {batch_index 3, batch_index_error_message "bad"}
            <<2, 3::32, 4, "bad", 0>>,
            # error_message "oops"
            <<5, "oops">>,
            # partition tags: 1 field, CurrentLeader (tag 0), size 9:
            # {leader_id 1, leader_epoch 4, empty tag buffer}
            <<1, 0, 9, 1::32, 4::32, 0>>,
            # topic tag buffer, throttle_time_ms 25
            <<0, 25::32>>,
            # root tags: 1 field, NodeEndpoints (tag 0), size 16:
            # compact array of 1 {node_id 1, host "host", port 9092, rack null, tags}
            <<1, 0, 16, 2, 1::32, 5, "host", 9092::32, 0, 0>>
          ])

        assert {response, <<>>} = response_module(vsn).deserialize(wire)
        assert response.throttle_time_ms == 25

        assert response.node_endpoints == [
                 %{node_id: 1, host: "host", port: 9092, rack: nil, tagged_fields: []}
               ]

        assert response.tagged_fields == []
        [topic] = response.responses
        assert Map.take(topic, [:name, :topic_id]) == topic_key(vsn)

        assert topic.partition_responses == [
                 %{
                   index: 0,
                   error_code: 10,
                   base_offset: -1,
                   log_append_time_ms: -1,
                   log_start_offset: -1,
                   record_errors: [
                     %{batch_index: 3, batch_index_error_message: "bad", tagged_fields: []}
                   ],
                   error_message: "oops",
                   current_leader: %{leader_id: 1, leader_epoch: 4, tagged_fields: []},
                   tagged_fields: []
                 }
               ]
      end
    end

    test "v13 absent tagged fields take their defaults" do
      wire =
        IO.iodata_to_binary([
          <<7::32, 0, 2>>,
          @topic_id,
          <<2, 0::32, 0::16, 0::64, -1::64-signed, 0::64, 1, 0, 0>>,
          <<0, 0::32, 0>>
        ])

      assert {response, <<>>} = response_module(13).deserialize(wire)
      assert response.node_endpoints == []
      [%{topic_id: @topic_id, partition_responses: [partition]}] = response.responses
      assert partition.current_leader == nil
    end
  end
end
