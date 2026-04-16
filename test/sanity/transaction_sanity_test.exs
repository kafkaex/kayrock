defmodule Kayrock.Sanity.TransactionSanityTest do
  @moduledoc """
  Sanity tests for transaction-related APIs.

  Covers:
  - InitProducerId V0-V2 (API key 22)
  - AddPartitionsToTxn V0-V1 (API key 24)
  - AddOffsetsToTxn V0-V1 (API key 25)
  - EndTxn V0-V1 (API key 26)
  - TxnOffsetCommit V0-V2 (API key 28)

  Run with:
      mix test.sanity test/sanity/transaction_sanity_test.exs

  Each API version gets its own transaction (unique transactional_id) to avoid
  cross-test interference. All requests requiring a coordinator are sent to
  :random — the Kayrock client will route them correctly.
  """
  use Kayrock.SanityCase
  use ExUnit.Case, async: false

  container(:kafka, KafkaContainer.new(), shared: true)

  # ---------------------------------------------------------------------------
  # Setup — one shared client + topic per test run
  # ---------------------------------------------------------------------------

  setup_all %{kafka: kafka} do
    {:ok, client} = build_client(kafka)
    topic = create_topic(client, 3)
    :ok = wait_for_topic(client, topic)
    %{client: client, topic: topic}
  end

  # ---------------------------------------------------------------------------
  # Private helpers
  # ---------------------------------------------------------------------------

  # Find the transaction coordinator for the given transactional_id.
  # Returns the node_id to use for all subsequent transaction requests.
  defp find_txn_coordinator(client, txn_id) do
    coord_vsn = min(2, Kayrock.FindCoordinator.max_vsn())
    coord_req = Kayrock.FindCoordinator.get_request_struct(coord_vsn)
    coord_req = %{coord_req | key: txn_id, key_type: 1}

    {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)

    assert coord_resp.error_code == 0,
           "FindCoordinator expected error_code 0, got #{coord_resp.error_code}"

    coord_resp.node_id
  end

  # Find the group coordinator for the given group_id.
  # TxnOffsetCommit must be sent to the group coordinator, not the txn coordinator.
  defp find_group_coordinator(client, group_id) do
    coord_vsn = min(2, Kayrock.FindCoordinator.max_vsn())
    coord_req = Kayrock.FindCoordinator.get_request_struct(coord_vsn)
    coord_req = %{coord_req | key: group_id, key_type: 0}

    {:ok, coord_resp} = with_retry(fn -> Kayrock.client_call(client, coord_req, :random) end)

    assert coord_resp.error_code == 0,
           "FindCoordinator(group) expected error_code 0, got #{coord_resp.error_code}"

    coord_resp.node_id
  end

  # Initialize a new producer and return {producer_id, producer_epoch, txn_id, node_id}.
  # Uses `version` for the InitProducerId request.
  # Discovers the transaction coordinator first and returns its node_id so all
  # subsequent transaction requests can be routed to the correct broker.
  defp init_producer(client, version, prefix) do
    txn_id = "#{prefix}-#{unique_string()}"
    node_id = find_txn_coordinator(client, txn_id)

    request = Kayrock.InitProducerId.get_request_struct(version)

    request =
      if version >= 2 do
        %{request | transactional_id: txn_id, transaction_timeout_ms: 30_000, tagged_fields: []}
      else
        %{request | transactional_id: txn_id, transaction_timeout_ms: 30_000}
      end

    {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)
    assert response.error_code == 0, "InitProducerId V#{version} expected error_code 0"
    {response.producer_id, response.producer_epoch, txn_id, node_id}
  end

  # Add a list of topic/partition pairs to the given transaction.
  defp add_partitions_to_txn(
         client,
         version,
         txn_id,
         producer_id,
         producer_epoch,
         topics_parts,
         node_id
       ) do
    topics =
      Enum.map(topics_parts, fn {topic, partitions} ->
        %{topic: topic, partitions: partitions}
      end)

    request = Kayrock.AddPartitionsToTxn.get_request_struct(version)

    request = %{
      request
      | transactional_id: txn_id,
        producer_id: producer_id,
        producer_epoch: producer_epoch,
        topics: topics
    }

    with_retry(fn -> Kayrock.client_call(client, request, node_id) end,
      accept_errors: [15, 16]
    )
  end

  # Add a consumer group to the given transaction.
  defp add_offsets_to_txn(
         client,
         version,
         txn_id,
         producer_id,
         producer_epoch,
         group_id,
         node_id
       ) do
    request = Kayrock.AddOffsetsToTxn.get_request_struct(version)

    request = %{
      request
      | transactional_id: txn_id,
        producer_id: producer_id,
        producer_epoch: producer_epoch,
        group_id: group_id
    }

    with_retry(fn -> Kayrock.client_call(client, request, node_id) end,
      accept_errors: [15, 16]
    )
  end

  # Commit or abort a transaction.
  defp end_txn(client, version, txn_id, producer_id, producer_epoch, commit?, node_id) do
    request = Kayrock.EndTxn.get_request_struct(version)

    request = %{
      request
      | transactional_id: txn_id,
        producer_id: producer_id,
        producer_epoch: producer_epoch,
        transaction_result: commit?
    }

    with_retry(fn -> Kayrock.client_call(client, request, node_id) end,
      accept_errors: [15, 16]
    )
  end

  # ---------------------------------------------------------------------------
  # InitProducerId V0-V2 (API key 22)
  # ---------------------------------------------------------------------------

  describe "InitProducerId" do
    for version <- 0..2 do
      @tag api: :init_producer_id, version: version
      test "V#{version} returns success with valid producer_id and epoch", %{kafka: kafka} do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        txn_id = "init-v#{version}-#{unique_string()}"
        node_id = find_txn_coordinator(client, txn_id)

        request = Kayrock.InitProducerId.get_request_struct(version)

        request =
          if version >= 2 do
            %{
              request
              | transactional_id: txn_id,
                transaction_timeout_ms: 30_000,
                tagged_fields: []
            }
          else
            %{request | transactional_id: txn_id, transaction_timeout_ms: 30_000}
          end

        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

        assert response.error_code == 0,
               "V#{version} expected error_code 0, got #{response.error_code}"

        assert is_integer(response.producer_id),
               "V#{version} producer_id should be integer"

        assert response.producer_id >= 0,
               "V#{version} producer_id should be non-negative, got #{response.producer_id}"

        assert is_integer(response.producer_epoch),
               "V#{version} producer_epoch should be integer"

        assert response.producer_epoch >= 0,
               "V#{version} producer_epoch should be non-negative"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0,
               "V#{version} throttle_time_ms should be non-negative"
      end
    end

    # V0-V1 use nullable_string for transactional_id
    for version <- 0..1 do
      @tag api: :init_producer_id, version: version
      test "V#{version} nil transactional_id (idempotent-only producer) succeeds", %{
        kafka: kafka
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        request = Kayrock.InitProducerId.get_request_struct(version)
        request = %{request | transactional_id: nil, transaction_timeout_ms: 30_000}

        {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, :random) end)

        assert response.error_code == 0,
               "V#{version} nil transactional_id expected error_code 0, got #{response.error_code}"

        assert response.producer_id >= 0
        assert response.producer_epoch >= 0
      end
    end

    # V2 is flexible — response must include tagged_fields
    @tag api: :init_producer_id, version: 2
    test "V2 response includes tagged_fields (flexible version)", %{kafka: kafka} do
      {:ok, client} = build_client(kafka)
      txn_id = "init-v2-flex-#{unique_string()}"
      node_id = find_txn_coordinator(client, txn_id)

      request = Kayrock.InitProducerId.get_request_struct(2)

      request = %{
        request
        | transactional_id: txn_id,
          transaction_timeout_ms: 30_000,
          tagged_fields: []
      }

      {:ok, response} = with_retry(fn -> Kayrock.client_call(client, request, node_id) end)

      assert response.error_code == 0
      assert is_list(response.tagged_fields), "V2 response.tagged_fields should be a list"
    end

    # Repeated calls with same transactional_id must bump epoch
    @tag api: :init_producer_id
    test "repeated InitProducerId for same transactional_id bumps producer_epoch", %{
      kafka: kafka
    } do
      {:ok, client} = build_client(kafka)
      txn_id = "epoch-bump-#{unique_string()}"
      node_id = find_txn_coordinator(client, txn_id)

      request1 = %{
        Kayrock.InitProducerId.get_request_struct(0)
        | transactional_id: txn_id,
          transaction_timeout_ms: 30_000
      }

      {:ok, r1} = with_retry(fn -> Kayrock.client_call(client, request1, node_id) end)
      assert r1.error_code == 0

      request2 = %{
        Kayrock.InitProducerId.get_request_struct(0)
        | transactional_id: txn_id,
          transaction_timeout_ms: 30_000
      }

      {:ok, r2} = with_retry(fn -> Kayrock.client_call(client, request2, node_id) end)
      assert r2.error_code == 0

      assert r2.producer_id == r1.producer_id,
             "Repeated init should return same producer_id"

      assert r2.producer_epoch > r1.producer_epoch,
             "Repeated init should bump epoch: #{r2.producer_epoch} > #{r1.producer_epoch}"
    end
  end

  # ---------------------------------------------------------------------------
  # AddPartitionsToTxn V0-V1 (API key 24)
  # ---------------------------------------------------------------------------

  describe "AddPartitionsToTxn" do
    for version <- 0..1 do
      @tag api: :add_partitions_to_txn, version: version
      test "V#{version} adds topic partition to transaction successfully", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        {producer_id, producer_epoch, txn_id, node_id} =
          init_producer(client, 0, "add-parts-v#{version}")

        {:ok, response} =
          add_partitions_to_txn(
            client,
            version,
            txn_id,
            producer_id,
            producer_epoch,
            [
              {topic, [0]}
            ],
            node_id
          )

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0

        # errors list: each entry has topic + partition_errors
        assert is_list(response.errors),
               "V#{version} response.errors should be a list"

        for error_entry <- response.errors do
          assert is_binary(error_entry.topic), "error_entry.topic should be binary"

          assert is_list(error_entry.partition_errors),
                 "error_entry.partition_errors should be list"

          for pe <- error_entry.partition_errors do
            assert is_integer(pe.partition), "partition_error.partition should be integer"
            assert pe.error_code == 0, "expected error_code 0, got #{pe.error_code}"
          end
        end

        # Abort the transaction to avoid dangling state
        end_txn(client, 0, txn_id, producer_id, producer_epoch, false, node_id)
      end
    end

    @tag api: :add_partitions_to_txn
    test "V0 adds multiple partitions in one call", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)
      {producer_id, producer_epoch, txn_id, node_id} = init_producer(client, 0, "add-multi-parts")

      {:ok, response} =
        add_partitions_to_txn(
          client,
          0,
          txn_id,
          producer_id,
          producer_epoch,
          [
            {topic, [0, 1, 2]}
          ],
          node_id
        )

      assert is_list(response.errors)

      # All 3 partitions should succeed (error_code 0)
      all_partition_errors =
        response.errors
        |> Enum.flat_map(& &1.partition_errors)

      for pe <- all_partition_errors do
        assert pe.error_code == 0,
               "Expected error_code 0 for partition #{pe.partition}, got #{pe.error_code}"
      end

      end_txn(client, 0, txn_id, producer_id, producer_epoch, false, node_id)
    end
  end

  # ---------------------------------------------------------------------------
  # AddOffsetsToTxn V0-V1 (API key 25)
  # ---------------------------------------------------------------------------

  describe "AddOffsetsToTxn" do
    for version <- 0..1 do
      @tag api: :add_offsets_to_txn, version: version
      test "V#{version} adds consumer group offsets to transaction", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        group_id = "txn-group-v#{version}-#{unique_string()}"

        {producer_id, producer_epoch, txn_id, node_id} =
          init_producer(client, 0, "add-offs-v#{version}")

        # Must add partitions to transaction first
        {:ok, _} =
          add_partitions_to_txn(
            client,
            0,
            txn_id,
            producer_id,
            producer_epoch,
            [{topic, [0]}],
            node_id
          )

        {:ok, response} =
          add_offsets_to_txn(
            client,
            version,
            txn_id,
            producer_id,
            producer_epoch,
            group_id,
            node_id
          )

        assert response.error_code == 0,
               "V#{version} AddOffsetsToTxn expected error_code 0, got #{response.error_code}"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0

        end_txn(client, 0, txn_id, producer_id, producer_epoch, false, node_id)
      end
    end

  end

  # ---------------------------------------------------------------------------
  # EndTxn V0-V1 (API key 26)
  # ---------------------------------------------------------------------------

  describe "EndTxn" do
    for version <- 0..1 do
      @tag api: :end_txn, version: version
      test "V#{version} commits transaction successfully (transaction_result: true)", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        {producer_id, producer_epoch, txn_id, node_id} =
          init_producer(client, 0, "end-commit-v#{version}")

        # Add a partition first so transaction is active
        {:ok, _} =
          add_partitions_to_txn(
            client,
            0,
            txn_id,
            producer_id,
            producer_epoch,
            [{topic, [0]}],
            node_id
          )

        {:ok, response} =
          end_txn(client, version, txn_id, producer_id, producer_epoch, true, node_id)

        assert response.error_code == 0,
               "V#{version} EndTxn commit expected error_code 0, got #{response.error_code}"

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0
      end

      @tag api: :end_txn, version: version
      test "V#{version} aborts transaction successfully (transaction_result: false)", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)

        {producer_id, producer_epoch, txn_id, node_id} =
          init_producer(client, 0, "end-abort-v#{version}")

        {:ok, _} =
          add_partitions_to_txn(
            client,
            0,
            txn_id,
            producer_id,
            producer_epoch,
            [{topic, [0]}],
            node_id
          )

        {:ok, response} =
          end_txn(client, version, txn_id, producer_id, producer_epoch, false, node_id)

        assert response.error_code == 0,
               "V#{version} EndTxn abort expected error_code 0, got #{response.error_code}"

        assert is_integer(response.throttle_time_ms)
        assert response.throttle_time_ms >= 0
      end
    end

  end

  # ---------------------------------------------------------------------------
  # TxnOffsetCommit V0-V2 (API key 28)
  # ---------------------------------------------------------------------------

  describe "TxnOffsetCommit" do
    for version <- 0..2 do
      @tag api: :txn_offset_commit, version: version
      test "V#{version} commits transactional offset successfully", %{
        kafka: kafka,
        topic: topic
      } do
        version = unquote(version)
        {:ok, client} = build_client(kafka)
        group_id = "txn-offset-group-v#{version}-#{unique_string()}"

        {producer_id, producer_epoch, txn_id, node_id} =
          init_producer(client, 0, "toc-v#{version}")

        # Must add partitions AND offsets-group to txn before TxnOffsetCommit
        {:ok, _} =
          add_partitions_to_txn(
            client,
            0,
            txn_id,
            producer_id,
            producer_epoch,
            [{topic, [0]}],
            node_id
          )

        {:ok, _} =
          add_offsets_to_txn(
            client,
            0,
            txn_id,
            producer_id,
            producer_epoch,
            group_id,
            node_id
          )

        # TxnOffsetCommit must go to the GROUP coordinator, not the txn coordinator
        group_node_id = find_group_coordinator(client, group_id)

        # Build the TxnOffsetCommit request
        request = Kayrock.TxnOffsetCommit.get_request_struct(version)

        partition_entry =
          if version >= 2 do
            %{
              partition_index: 0,
              committed_offset: 0,
              committed_leader_epoch: -1,
              committed_metadata: ""
            }
          else
            %{partition_index: 0, committed_offset: 0, committed_metadata: ""}
          end

        request = %{
          request
          | transactional_id: txn_id,
            group_id: group_id,
            producer_id: producer_id,
            producer_epoch: producer_epoch,
            topics: [
              %{
                name: topic,
                partitions: [partition_entry]
              }
            ]
        }

        {:ok, response} =
          with_retry(fn -> Kayrock.client_call(client, request, group_node_id) end,
            accept_errors: [15, 16]
          )

        assert is_integer(response.throttle_time_ms),
               "V#{version} throttle_time_ms should be integer"

        assert response.throttle_time_ms >= 0

        # response.topics is a list of topic-level results
        assert is_list(response.topics),
               "V#{version} response.topics should be a list"

        for topic_entry <- response.topics do
          assert is_binary(topic_entry.name), "topic_entry.name should be binary"
          assert is_list(topic_entry.partitions), "topic_entry.partitions should be list"

          for part <- topic_entry.partitions do
            assert is_integer(part.partition_index), "partition_index should be integer"

            assert part.error_code == 0,
                   "V#{version} partition #{part.partition_index} expected error_code 0, got #{part.error_code}"
          end
        end

        # Commit the transaction
        end_txn(client, 0, txn_id, producer_id, producer_epoch, true, node_id)
      end
    end


    @tag api: :txn_offset_commit
    test "V2 adds committed_leader_epoch in partition entry", %{kafka: kafka, topic: topic} do
      {:ok, client} = build_client(kafka)
      group_id = "txn-v2-epoch-group-#{unique_string()}"
      {producer_id, producer_epoch, txn_id, node_id} = init_producer(client, 0, "toc-v2-epoch")

      {:ok, _} =
        add_partitions_to_txn(
          client,
          0,
          txn_id,
          producer_id,
          producer_epoch,
          [{topic, [0]}],
          node_id
        )

      {:ok, _} =
        add_offsets_to_txn(client, 0, txn_id, producer_id, producer_epoch, group_id, node_id)

      group_node_id = find_group_coordinator(client, group_id)

      request = %{
        Kayrock.TxnOffsetCommit.get_request_struct(2)
        | transactional_id: txn_id,
          group_id: group_id,
          producer_id: producer_id,
          producer_epoch: producer_epoch,
          topics: [
            %{
              name: topic,
              partitions: [
                %{
                  partition_index: 0,
                  committed_offset: 0,
                  committed_leader_epoch: -1,
                  committed_metadata: ""
                }
              ]
            }
          ]
      }

      {:ok, response} =
        with_retry(fn -> Kayrock.client_call(client, request, group_node_id) end,
          accept_errors: [15, 16]
        )

      assert is_list(response.topics)

      for t <- response.topics, p <- t.partitions do
        assert p.error_code == 0,
               "V2 partition #{p.partition_index} expected error_code 0, got #{p.error_code}"
      end

      end_txn(client, 0, txn_id, producer_id, producer_epoch, true, node_id)
    end
  end

  # ---------------------------------------------------------------------------
  # Full transaction lifecycle (cross-API)
  # ---------------------------------------------------------------------------

  describe "Transaction lifecycle" do
    @tag api: :transaction_lifecycle
    test "full lifecycle: InitProducerId -> AddPartitions -> Produce -> EndTxn (commit)", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)

      {producer_id, producer_epoch, txn_id, node_id} =
        init_producer(client, 0, "lifecycle-commit")

      # Register partition
      {:ok, add_resp} =
        add_partitions_to_txn(
          client,
          0,
          txn_id,
          producer_id,
          producer_epoch,
          [{topic, [0]}],
          node_id
        )

      assert is_list(add_resp.errors)

      # Commit
      {:ok, end_resp} = end_txn(client, 0, txn_id, producer_id, producer_epoch, true, node_id)
      assert end_resp.error_code == 0
    end

    @tag api: :transaction_lifecycle
    test "full lifecycle: InitProducerId -> AddPartitions -> EndTxn (abort)", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)
      {producer_id, producer_epoch, txn_id, node_id} = init_producer(client, 0, "lifecycle-abort")

      {:ok, _} =
        add_partitions_to_txn(
          client,
          0,
          txn_id,
          producer_id,
          producer_epoch,
          [{topic, [0]}],
          node_id
        )

      {:ok, end_resp} = end_txn(client, 0, txn_id, producer_id, producer_epoch, false, node_id)
      assert end_resp.error_code == 0
    end

    @tag api: :transaction_lifecycle
    test "full lifecycle with AddOffsetsToTxn and TxnOffsetCommit", %{
      kafka: kafka,
      topic: topic
    } do
      {:ok, client} = build_client(kafka)
      group_id = "lifecycle-full-group-#{unique_string()}"
      {producer_id, producer_epoch, txn_id, node_id} = init_producer(client, 0, "lifecycle-full")

      # Step 1: add partition
      {:ok, _} =
        add_partitions_to_txn(
          client,
          0,
          txn_id,
          producer_id,
          producer_epoch,
          [{topic, [0]}],
          node_id
        )

      # Step 2: add offsets group
      {:ok, offs_resp} =
        add_offsets_to_txn(client, 0, txn_id, producer_id, producer_epoch, group_id, node_id)

      assert offs_resp.error_code == 0

      # Step 3: commit transactional offset (must route to GROUP coordinator)
      group_node_id = find_group_coordinator(client, group_id)

      request = %{
        Kayrock.TxnOffsetCommit.get_request_struct(0)
        | transactional_id: txn_id,
          group_id: group_id,
          producer_id: producer_id,
          producer_epoch: producer_epoch,
          topics: [
            %{
              name: topic,
              partitions: [%{partition_index: 0, committed_offset: 0, committed_metadata: ""}]
            }
          ]
      }

      {:ok, toc_resp} =
        with_retry(fn -> Kayrock.client_call(client, request, group_node_id) end,
          accept_errors: [15, 16]
        )

      assert is_list(toc_resp.topics)

      # Step 4: end transaction
      {:ok, end_resp} = end_txn(client, 0, txn_id, producer_id, producer_epoch, true, node_id)
      assert end_resp.error_code == 0
    end
  end
end
