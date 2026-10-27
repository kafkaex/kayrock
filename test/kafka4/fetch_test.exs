defmodule Kayrock.Kafka4.FetchTest do
  @moduledoc """
  Fetch v12-v18 (JSON-derived) against `apache/kafka:4.4.0`: every new
  version is accepted, the response decodes with no leftover bytes, and the
  records produced (at Produce v13) come back through `compact_records`.
  v12 fetches by topic name; v13+ fetch by topic ID. Also covers incremental
  fetch sessions with `forgotten_topics_data` (`[]int32` partitions).
  """
  use Kayrock.Kafka4Case

  alias Kayrock.Kafka4Wire
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  setup %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_kafka4_topic(client, partitions: 2)
    :ok = Kayrock.Client.stop(client)
    topic_id = Kafka4Wire.topic_id(kafka, topic)

    records =
      for i <- 1..5 do
        %Record{key: "k#{i}", value: "m#{i}", headers: [%RecordHeader{key: "h", value: "#{i}"}]}
      end

    for {index, batch} <- [{0, records}, {1, [%Record{value: "p1"}]}] do
      produce = %Kayrock.Produce.V13.Request{
        transactional_id: nil,
        acks: -1,
        timeout_ms: 5000,
        topic_data: [
          %{
            topic_id: topic_id,
            partition_data: [%{index: index, records: %RecordBatch{records: batch}}]
          }
        ]
      }

      %{responses: [%{partition_responses: [%{error_code: 0}]}]} =
        Kafka4Wire.produce(kafka, produce)
    end

    {:ok, topic: topic, topic_id: topic_id}
  end

  defp topic_key(vsn, topic, _topic_id) when vsn < 13, do: %{topic: topic}
  defp topic_key(_vsn, _topic, topic_id), do: %{topic_id: topic_id}

  defp partition(index, offset) do
    %{
      partition: index,
      current_leader_epoch: -1,
      fetch_offset: offset,
      last_fetched_epoch: -1,
      log_start_offset: -1,
      partition_max_bytes: 1_000_000
    }
  end

  defp fetch_request(vsn, topic_key, partitions, opts \\ []) do
    fields = [
      max_wait_ms: 100,
      min_bytes: 0,
      max_bytes: 1_000_000,
      isolation_level: 1,
      session_id: Keyword.get(opts, :session_id, 0),
      session_epoch: Keyword.get(opts, :session_epoch, -1),
      topics: if(partitions == [], do: [], else: [Map.put(topic_key, :partitions, partitions)]),
      forgotten_topics_data: Keyword.get(opts, :forgotten, []),
      rack_id: ""
    ]

    fields = if vsn < 15, do: [{:replica_id, -1} | fields], else: fields
    struct(Kayrock.Fetch.get_request_struct(vsn), fields)
  end

  for vsn <- 12..18 do
    test "Fetch v#{vsn} returns the produced records", %{
      kafka: kafka,
      topic: topic,
      topic_id: topic_id
    } do
      vsn = unquote(vsn)
      key = topic_key(vsn, topic, topic_id)

      response = Kafka4Wire.call(kafka, fetch_request(vsn, key, [partition(0, 2)]))
      assert response.error_code == 0
      assert response.session_id == 0
      if vsn >= 16, do: assert(response.node_endpoints == [])

      [topic_response] = response.responses
      assert Map.take(topic_response, [:topic, :topic_id]) == key

      [p] = topic_response.partitions
      assert p.partition_index == 0
      assert p.error_code == 0
      assert p.high_watermark == 5
      assert p.last_stable_offset == 5
      assert p.log_start_offset == 0
      assert p.preferred_read_replica == -1
      assert p.diverging_epoch == nil
      assert p.snapshot_id == nil

      [%RecordBatch{} = batch] = p.records
      assert batch.batch_offset == 0
      assert Enum.map(batch.records, & &1.offset) == [0, 1, 2, 3, 4]
      assert Enum.map(batch.records, & &1.value) == ~w(m1 m2 m3 m4 m5)
      assert Enum.map(batch.records, & &1.key) == ~w(k1 k2 k3 k4 k5)
      assert hd(batch.records).headers == [%RecordHeader{key: "h", value: "1"}]
    end
  end

  test "Fetch v18 past the high watermark returns no records", %{
    kafka: kafka,
    topic_id: topic_id
  } do
    response =
      Kafka4Wire.call(kafka, fetch_request(18, %{topic_id: topic_id}, [partition(0, 5)]))

    [%{partitions: [p]}] = response.responses
    assert p.error_code == 0
    assert p.records in [nil, []]
  end

  test "Fetch v13 with an unknown topic ID returns UNKNOWN_TOPIC_ID", %{kafka: kafka} do
    unknown = :crypto.strong_rand_bytes(16)

    response =
      Kafka4Wire.call(kafka, fetch_request(13, %{topic_id: unknown}, [partition(0, 0)]))

    [%{topic_id: ^unknown, partitions: [p]}] = response.responses
    assert Kayrock.ErrorCode.code_to_atom(p.error_code) == :unknown_topic_id
  end

  for vsn <- [12, 18] do
    test "Fetch v#{vsn} incremental session forgets a partition via forgotten_topics_data",
         %{kafka: kafka, topic: topic, topic_id: topic_id} do
      vsn = unquote(vsn)
      key = topic_key(vsn, topic, topic_id)
      {:ok, conn} = connect_broker(kafka)

      try do
        # Full fetch that opens a session (session_id 0, epoch 0)
        first =
          session_call(
            conn,
            fetch_request(vsn, key, [partition(0, 0), partition(1, 0)], session_epoch: 0)
          )

        assert first.error_code == 0
        assert first.session_id > 0
        [%{partitions: parts}] = first.responses
        assert Enum.sort(Enum.map(parts, & &1.partition_index)) == [0, 1]

        # Incremental fetch: forget partition 1 ([]int32), nothing else changes
        second =
          session_call(
            conn,
            fetch_request(vsn, key, [],
              session_id: first.session_id,
              session_epoch: 1,
              forgotten: [Map.put(key, :partitions, [1])]
            )
          )

        assert second.error_code == 0
        assert second.session_id == first.session_id

        # Partition 1 must no longer be part of the session
        third =
          session_call(
            conn,
            fetch_request(vsn, key, [], session_id: first.session_id, session_epoch: 2)
          )

        assert third.error_code == 0

        indexes =
          for %{partitions: parts} <- third.responses, p <- parts, do: p.partition_index

        refute 1 in indexes
      after
        Kayrock.BrokerConnection.stop(conn)
      end
    end
  end

  # Same as Kafka4Wire.call/2 but on a shared connection (fetch sessions are
  # not bound to a connection, but keeping one is closer to a real consumer)
  defp session_call(conn, %module{} = request) do
    request = %{request | correlation_id: 99, client_id: "kayrock-kafka4"}
    :ok = Kayrock.BrokerConnection.send(conn, module.serialize(request))
    {:ok, data} = Kayrock.BrokerConnection.recv(conn, 10_000)
    {response, <<>>} = module.response_deserializer().(data)
    response
  end
end
