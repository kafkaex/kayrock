defmodule Kayrock.Kafka4.TransactionsTest do
  @moduledoc """
  Transactional producer flows against `apache/kafka:4.4.0` at the
  JSON-derived versions of InitProducerId, AddPartitionsToTxn,
  AddOffsetsToTxn, EndTxn and TxnOffsetCommit, plus the admin APIs
  DescribeProducers, DescribeTransactions and ListTransactions.

  Every request goes over a raw `Kayrock.BrokerConnection` (single node: it
  is the transaction and group coordinator and the partition leader), and
  every response must deserialize with no bytes left over.

  The transactional produce uses Produce v8 (the highest generated version)
  with a transactional record batch.

  TxnOffsetCommit v6 identifies topics by ID. The generated Metadata (v9) does
  not return topic IDs, so a test-only Metadata v12 is compiled from the JSON
  specs with `Kayrock.Test.GeneratedCode` (nothing is added to
  `lib/generated`).
  """
  use Kayrock.Kafka4Case

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.Test.GeneratedCode

  defmodule MetadataV12Schema do
    @moduledoc false
    # Serves Metadata v12 (returns topic IDs) straight from the JSON specs.
    alias Kayrock.Generate.ApiTable
    alias Kayrock.Generate.JsonSpec

    defp spec, do: JsonSpec.load!(ApiTable.spec_dir())

    def vsn_range(:metadata), do: {12, 12}
    def min_flexible_vsn(api), do: JsonSpec.min_flexible_version(spec(), api)
    def api_key(api), do: JsonSpec.api_key(spec(), api)
    def req(api, vsn), do: JsonSpec.request_schema(spec(), api, vsn)
    def rsp(api, vsn), do: JsonSpec.response_schema(spec(), api, vsn)
    def request_header_vsn(api, vsn), do: JsonSpec.request_header_version(spec(), api, vsn)
    def response_header_vsn(api, vsn), do: JsonSpec.response_header_version(spec(), api, vsn)
  end

  @gen Kayrock.Kafka4.TransactionsTest.Gen

  # COORDINATOR_LOAD_IN_PROGRESS, COORDINATOR_NOT_AVAILABLE, NOT_COORDINATOR,
  # CONCURRENT_TRANSACTIONS
  @retriable [14, 15, 16, 51]

  # The transactional bit of RecordBatch attributes
  @transactional_attribute 0x10

  # Versions of each flow, oldest JSON-derived version first, ending at the
  # highest generated version of every API
  @flows [
    %{init: 3, add_partitions: 2, add_offsets: 2, end_txn: 2, txn_offset_commit: 3},
    %{init: 4, add_partitions: 3, add_offsets: 3, end_txn: 3, txn_offset_commit: 4},
    %{init: 5, add_partitions: 3, add_offsets: 4, end_txn: 4, txn_offset_commit: 5},
    %{init: 5, add_partitions: 3, add_offsets: 4, end_txn: 5, txn_offset_commit: 6}
  ]

  setup_all do
    GeneratedCode.compile!(:metadata, MetadataV12Schema, @gen)
    :ok
  end

  setup %{kafka: kafka} do
    {:ok, conn} = connect_broker(kafka)
    Process.unlink(conn)
    on_exit(fn -> Process.exit(conn, :kill) end)

    {:ok, client} = build_client(kafka)

    # The transaction state and offsets topics are created on the first
    # FindCoordinator for a transactional id / group; wait until both
    # coordinators are ready
    find_coordinator(conn, 1, "kayrock-warmup")
    find_coordinator(conn, 0, "kayrock-warmup")

    {:ok, conn: conn, client: client}
  end

  test "the flows cover every JSON-derived version" do
    expected = %{
      init: 3..Kayrock.InitProducerId.max_vsn(),
      add_partitions: 2..Kayrock.AddPartitionsToTxn.max_vsn(),
      add_offsets: 2..Kayrock.AddOffsetsToTxn.max_vsn(),
      end_txn: 2..Kayrock.EndTxn.max_vsn(),
      txn_offset_commit: 3..Kayrock.TxnOffsetCommit.max_vsn()
    }

    for {key, range} <- expected do
      assert @flows |> Enum.map(& &1[key]) |> Enum.uniq() == Enum.to_list(range), "#{key}"
    end
  end

  for flow <- @flows do
    test "commit flow: InitProducerId v#{flow.init}, AddPartitionsToTxn v#{flow.add_partitions}, " <>
           "AddOffsetsToTxn v#{flow.add_offsets}, TxnOffsetCommit v#{flow.txn_offset_commit}, " <>
           "EndTxn v#{flow.end_txn}",
         %{conn: conn, client: client} do
      flow = unquote(Macro.escape(flow))
      topic = create_kafka4_topic(client, partitions: 2)
      txn_id = "kayrock-txn-#{System.unique_integer([:positive])}"
      group_id = "kayrock-txn-group-#{System.unique_integer([:positive])}"

      %{producer_id: pid, producer_epoch: epoch} = init_producer_id(conn, txn_id, flow.init)
      assert pid >= 0
      assert epoch >= 0

      add_partitions(conn, flow.add_partitions, txn_id, pid, epoch, topic, [0, 1])
      assert transactional_produce(conn, txn_id, pid, epoch, topic, 0, ["a", "b"]) == 0
      assert transactional_produce(conn, txn_id, pid, epoch, topic, 1, ["c"]) == 0

      add_offsets(conn, flow.add_offsets, txn_id, pid, epoch, group_id)
      txn_offset_commit(conn, flow.txn_offset_commit, txn_id, group_id, pid, epoch, topic, 2)

      # DescribeTransactions v0: ongoing, with both partitions of the topic
      [state] = describe_transactions(conn, [txn_id])
      assert state.error_code == 0
      assert state.transactional_id == txn_id
      assert state.transaction_state == "Ongoing"
      assert state.transaction_timeout_ms == 60_000
      assert state.producer_id == pid
      assert state.producer_epoch == epoch
      assert state.transaction_start_time_ms > 0

      assert %{topic: ^topic, partitions: partitions} =
               Enum.find(state.topics, &(&1.topic == topic))

      assert Enum.sort(partitions) == [0, 1]

      # DescribeProducers v0: the producer is active on both partitions
      [%{name: ^topic, partitions: producer_partitions}] =
        describe_producers(conn, topic, [0, 1])

      for %{error_code: 0, active_producers: active} <- producer_partitions do
        assert [producer] = Enum.filter(active, &(&1.producer_id == pid))
        assert producer.producer_epoch == epoch
        assert producer.current_txn_start_offset == 0
      end

      # EndTxn: commit
      end_response = end_txn(conn, flow.end_txn, txn_id, pid, epoch, true)

      if flow.end_txn >= 5 do
        # KIP-890 part 2: the epoch is bumped on every EndTxn v5
        assert end_response.producer_id == pid
        assert end_response.producer_epoch == epoch + 1
      end

      assert wait_for_state(conn, txn_id, "CompleteCommit") == :ok
    end
  end

  test "abort flow at the highest versions", %{conn: conn, client: client} do
    topic = create_kafka4_topic(client, partitions: 1)
    txn_id = "kayrock-txn-#{System.unique_integer([:positive])}"
    init_vsn = Kayrock.InitProducerId.max_vsn()

    %{producer_id: pid, producer_epoch: epoch} = init_producer_id(conn, txn_id, init_vsn)
    add_partitions(conn, Kayrock.AddPartitionsToTxn.max_vsn(), txn_id, pid, epoch, topic, [0])
    assert transactional_produce(conn, txn_id, pid, epoch, topic, 0, ["x"]) == 0

    end_txn(conn, Kayrock.EndTxn.max_vsn(), txn_id, pid, epoch, false)
    assert wait_for_state(conn, txn_id, "CompleteAbort") == :ok

    # InitProducerId v3+ with the current producer id/epoch (KIP-360)
    resumed =
      init_producer_id(conn, txn_id, init_vsn, producer_id: pid, producer_epoch: epoch + 1)

    assert resumed.producer_id == pid
    assert resumed.producer_epoch > epoch + 1
  end

  test "ListTransactions v0-v2 filters", %{conn: conn, client: client} do
    topic = create_kafka4_topic(client, partitions: 1)
    txn_id = "kayrock-list-#{System.unique_integer([:positive])}"

    %{producer_id: pid, producer_epoch: epoch} =
      init_producer_id(conn, txn_id, Kayrock.InitProducerId.max_vsn())

    add_partitions(conn, Kayrock.AddPartitionsToTxn.max_vsn(), txn_id, pid, epoch, topic, [0])
    assert transactional_produce(conn, txn_id, pid, epoch, topic, 0, ["x"]) == 0

    ours = %{transactional_id: txn_id, producer_id: pid, transaction_state: "Ongoing"}

    for version <- 0..Kayrock.ListTransactions.max_vsn() do
      # state + producer id filters (v0+)
      response =
        list_transactions(conn, version, state_filters: ["Ongoing"], producer_id_filters: [pid])

      assert response.error_code == 0
      assert response.unknown_state_filters == []
      assert [listed] = response.transaction_states
      assert Map.take(listed, Map.keys(ours)) == ours

      # unknown state filters are echoed back
      response = list_transactions(conn, version, state_filters: ["Ongoing", "NotAState"])
      assert response.unknown_state_filters == ["NotAState"]
      assert Enum.any?(response.transaction_states, &(&1.transactional_id == txn_id))

      # an unrelated producer id matches nothing
      response = list_transactions(conn, version, producer_id_filters: [pid + 1_000_000])
      assert response.transaction_states == []
    end

    # DurationFilter (v1+): only transactions older than the duration
    response = list_transactions(conn, 1, duration_filter: 0)
    assert Enum.any?(response.transaction_states, &(&1.transactional_id == txn_id))
    response = list_transactions(conn, 1, duration_filter: 3_600_000)
    refute Enum.any?(response.transaction_states, &(&1.transactional_id == txn_id))

    # TransactionalIdPattern (v2+, KIP-1152)
    response = list_transactions(conn, 2, transactional_id_pattern: Regex.escape(txn_id))
    assert [%{transactional_id: ^txn_id}] = response.transaction_states
    response = list_transactions(conn, 2, transactional_id_pattern: "^no-such-txn-.*")
    assert response.transaction_states == []

    end_txn(conn, Kayrock.EndTxn.max_vsn(), txn_id, pid, epoch, false)
  end

  test "DescribeTransactions v0 reports an unknown transactional id", %{conn: conn} do
    txn_id = "kayrock-missing-#{System.unique_integer([:positive])}"
    # TRANSACTIONAL_ID_NOT_FOUND
    assert [%{transactional_id: ^txn_id, error_code: 105}] =
             with_retry(fn ->
               case describe_transactions(conn, [txn_id]) do
                 [%{error_code: code}] when code in @retriable -> :retry
                 states -> {:ok, states}
               end
             end)

    assert describe_transactions(conn, []) == []
  end

  test "DescribeProducers v0 reports an unknown partition", %{conn: conn, client: client} do
    topic = create_kafka4_topic(client, partitions: 1)

    # NOT_LEADER_OR_FOLLOWER right after topic creation
    [%{name: ^topic, partitions: [ok, missing]}] =
      with_retry(fn ->
        case describe_producers(conn, topic, [0, 7]) do
          [%{partitions: [%{error_code: 6} | _]}] -> :retry
          topics -> {:ok, topics}
        end
      end)

    assert %{partition_index: 0, error_code: 0, active_producers: []} = ok
    # UNKNOWN_TOPIC_OR_PARTITION
    assert %{partition_index: 7, error_code: 3, active_producers: []} = missing
  end

  # ---------------------------------------------------------------------------
  # Requests

  # key_type: 0 = group, 1 = transaction
  defp find_coordinator(conn, key_type, key) do
    request = %Kayrock.FindCoordinator.V3.Request{key: key, key_type: key_type, tagged_fields: []}
    assert %{node_id: node_id} = retry_call(conn, request)
    assert node_id >= 0
  end

  defp init_producer_id(conn, txn_id, version, opts \\ []) do
    request = %{
      Kayrock.InitProducerId.get_request_struct(version)
      | transactional_id: txn_id,
        transaction_timeout_ms: 60_000,
        producer_id: Keyword.get(opts, :producer_id, -1),
        producer_epoch: Keyword.get(opts, :producer_epoch, -1)
    }

    response = retry_call(conn, request)
    assert response.throttle_time_ms >= 0
    response
  end

  defp add_partitions(conn, version, txn_id, pid, epoch, topic, partitions) do
    request = %{
      Kayrock.AddPartitionsToTxn.get_request_struct(version)
      | v3_and_below_transactional_id: txn_id,
        v3_and_below_producer_id: pid,
        v3_and_below_producer_epoch: epoch,
        v3_and_below_topics: [topic_partitions(version, topic, partitions)]
    }

    response = call(conn, request)
    [%{name: ^topic, results_by_partition: results}] = response.results_by_topic_v3_and_below

    assert results |> Enum.map(& &1.partition_index) |> Enum.sort() == partitions
    assert Enum.all?(results, &(&1.partition_error_code == 0)), inspect(results)
    response
  end

  defp topic_partitions(version, topic, partitions) when version >= 3,
    do: %{name: topic, partitions: partitions, tagged_fields: []}

  defp topic_partitions(_version, topic, partitions), do: %{name: topic, partitions: partitions}

  defp transactional_produce(conn, txn_id, pid, epoch, topic, partition, values) do
    batch = %RecordBatch{
      attributes: @transactional_attribute,
      producer_id: pid,
      producer_epoch: epoch,
      base_sequence: 0,
      records: Enum.map(values, &%Record{value: &1})
    }

    request = %{
      Kayrock.Produce.get_request_struct(Kayrock.Produce.max_vsn())
      | transactional_id: txn_id,
        acks: -1,
        timeout: 10_000,
        topic_data: [%{topic: topic, data: [%{partition: partition, record_set: batch}]}]
    }

    # NOT_LEADER_OR_FOLLOWER right after topic creation
    result =
      with_retry(fn ->
        %{responses: [%{topic: ^topic, partition_responses: [result]}]} = call(conn, request)
        if result.error_code == 6, do: :retry, else: {:ok, result}
      end)

    assert result.error_code == 0, inspect(result)
    result.base_offset
  end

  defp add_offsets(conn, version, txn_id, pid, epoch, group_id) do
    request = %{
      Kayrock.AddOffsetsToTxn.get_request_struct(version)
      | transactional_id: txn_id,
        producer_id: pid,
        producer_epoch: epoch,
        group_id: group_id
    }

    assert %{error_code: 0} = retry_call(conn, request)
  end

  defp txn_offset_commit(conn, version, txn_id, group_id, pid, epoch, topic, offset) do
    partitions =
      for index <- [0, 1] do
        partition = %{
          partition_index: index,
          committed_offset: offset,
          committed_leader_epoch: -1,
          committed_metadata: "kayrock"
        }

        if version >= 3, do: Map.put(partition, :tagged_fields, []), else: partition
      end

    topic_entry =
      if version >= 6,
        do: %{topic_id: topic_id(conn, topic), partitions: partitions, tagged_fields: []},
        else: %{name: topic, partitions: partitions, tagged_fields: []}

    request = %{
      Kayrock.TxnOffsetCommit.get_request_struct(version)
      | transactional_id: txn_id,
        group_id: group_id,
        producer_id: pid,
        producer_epoch: epoch,
        generation_id_or_member_epoch: -1,
        member_id: "",
        group_instance_id: nil,
        topics: [topic_entry]
    }

    response =
      with_retry(fn ->
        response = call(conn, request)
        codes = for t <- response.topics, p <- t.partitions, do: p.error_code
        if Enum.any?(codes, &(&1 in @retriable)), do: :retry, else: {:ok, response}
      end)

    [result] = response.topics

    if version >= 6,
      do: assert(result.topic_id == topic_entry.topic_id),
      else: assert(result.name == topic)

    assert result.partitions |> Enum.map(&{&1.partition_index, &1.error_code}) |> Enum.sort() ==
             [{0, 0}, {1, 0}]
  end

  defp end_txn(conn, version, txn_id, pid, epoch, committed?) do
    request = %{
      Kayrock.EndTxn.get_request_struct(version)
      | transactional_id: txn_id,
        producer_id: pid,
        producer_epoch: epoch,
        committed: committed?
    }

    response = retry_call(conn, request)
    assert response.error_code == 0
    response
  end

  defp describe_transactions(conn, txn_ids) do
    request = %Kayrock.DescribeTransactions.V0.Request{transactional_ids: txn_ids}
    call(conn, request).transaction_states
  end

  defp describe_producers(conn, topic, partitions) do
    request = %Kayrock.DescribeProducers.V0.Request{
      topics: [%{name: topic, partition_indexes: partitions, tagged_fields: []}]
    }

    call(conn, request).topics
  end

  defp list_transactions(conn, version, filters) do
    defaults = [
      state_filters: [],
      producer_id_filters: [],
      duration_filter: -1,
      transactional_id_pattern: nil
    ]

    # struct/2 ignores fields a version does not have
    request =
      struct(
        Kayrock.ListTransactions.get_request_struct(version),
        Keyword.merge(defaults, filters)
      )

    response = call(conn, request)
    assert response.throttle_time_ms >= 0
    response
  end

  defp wait_for_state(conn, txn_id, state) do
    with_retry(fn ->
      case describe_transactions(conn, [txn_id]) do
        [%{error_code: 0, transaction_state: ^state}] -> {:ok, :ok}
        _ -> :retry
      end
    end)
  end

  # Metadata v12 (test-only, JSON-derived) to look up a topic's ID
  defp topic_id(conn, topic) do
    request_module = GeneratedCode.module(@gen, :metadata, 12, Request)
    response_module = GeneratedCode.module(@gen, :metadata, 12, Response)

    request =
      struct(request_module,
        topics: [%{topic_id: nil, name: topic, tagged_fields: []}],
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: false
      )

    {response, rest} = raw_call(conn, request, &response_module.deserialize/1)
    assert rest == <<>>
    [%{name: ^topic, error_code: 0, topic_id: topic_id}] = response.topics
    assert byte_size(topic_id) == 16 and topic_id != <<0::128>>
    topic_id
  end

  # ---------------------------------------------------------------------------
  # Plumbing

  # Retries while the top-level error code is retriable
  defp retry_call(conn, request) do
    with_retry(fn ->
      case call(conn, request) do
        %{error_code: code} when code in @retriable -> :retry
        response -> {:ok, response}
      end
    end)
  end

  defp with_retry(fun, attempts \\ 300)
  defp with_retry(_fun, 0), do: flunk("gave up retrying")

  defp with_retry(fun, attempts) do
    case fun.() do
      {:ok, result} ->
        result

      :retry ->
        Process.sleep(100)
        with_retry(fun, attempts - 1)
    end
  end

  # Sends a generated request and asserts that the response decodes fully
  defp call(conn, request) do
    {response, rest} = raw_call(conn, request, Kayrock.Request.response_deserializer(request))

    assert rest == <<>>,
           "#{inspect(request.__struct__)}: #{byte_size(rest)} bytes left over"

    response
  end

  defp raw_call(conn, %module{} = request, deserialize) do
    correlation_id = rem(System.unique_integer([:positive]), 1_000_000)
    request = %{request | correlation_id: correlation_id, client_id: "kayrock-kafka4"}
    :ok = Kayrock.BrokerConnection.send(conn, module.serialize(request))
    {:ok, data} = Kayrock.BrokerConnection.recv(conn, 15_000)
    {response, rest} = deserialize.(data)
    assert response.correlation_id == correlation_id
    {response, rest}
  end
end
