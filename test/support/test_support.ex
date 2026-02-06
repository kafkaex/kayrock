defmodule Kayrock.TestSupport do
  @moduledoc "Support code for tests"

  @doc """
  Returns the expected version range (min..max) for a given Kafka API.

  These ranges are based on the generated modules from kafka_protocol.
  Tests should use these ranges instead of blindly iterating over versions.
  """
  @spec api_version_range(atom()) :: Range.t()
  # Core APIs
  def api_version_range(:produce), do: 0..8
  def api_version_range(:fetch), do: 0..11
  def api_version_range(:list_offsets), do: 0..5
  def api_version_range(:metadata), do: 0..9

  # Offset management
  def api_version_range(:offset_commit), do: 0..8
  def api_version_range(:offset_fetch), do: 0..6

  # Consumer groups
  def api_version_range(:find_coordinator), do: 0..3
  def api_version_range(:join_group), do: 0..6
  def api_version_range(:heartbeat), do: 0..4
  def api_version_range(:leave_group), do: 0..4
  def api_version_range(:sync_group), do: 0..4
  def api_version_range(:describe_groups), do: 0..5
  def api_version_range(:list_groups), do: 0..3
  def api_version_range(:delete_groups), do: 0..2

  # Admin: Topics
  def api_version_range(:create_topics), do: 0..5
  def api_version_range(:delete_topics), do: 0..4
  def api_version_range(:create_partitions), do: 0..1
  def api_version_range(:delete_records), do: 0..1

  # Admin: Config
  def api_version_range(:describe_configs), do: 0..2
  def api_version_range(:alter_configs), do: 0..1
  def api_version_range(:incremental_alter_configs), do: 0..1

  # Admin: Logs
  def api_version_range(:alter_replica_log_dirs), do: 0..1
  def api_version_range(:describe_log_dirs), do: 0..1

  # Admin: ACLs
  def api_version_range(:describe_acls), do: 0..1
  def api_version_range(:create_acls), do: 0..1
  def api_version_range(:delete_acls), do: 0..1

  # Admin: Partition reassignment
  def api_version_range(:alter_partition_reassignments), do: 0..0
  def api_version_range(:list_partition_reassignments), do: 0..0
  def api_version_range(:elect_leaders), do: 0..2

  # Admin: Delegation tokens
  def api_version_range(:create_delegation_token), do: 0..2
  def api_version_range(:renew_delegation_token), do: 0..1
  def api_version_range(:expire_delegation_token), do: 0..1
  def api_version_range(:describe_delegation_token), do: 0..1

  # Transactions
  def api_version_range(:init_producer_id), do: 0..2
  def api_version_range(:add_partitions_to_txn), do: 0..1
  def api_version_range(:add_offsets_to_txn), do: 0..1
  def api_version_range(:end_txn), do: 0..1
  def api_version_range(:txn_offset_commit), do: 0..2
  def api_version_range(:offset_delete), do: 0..0

  # Leader epoch
  def api_version_range(:offset_for_leader_epoch), do: 0..3

  # SASL
  def api_version_range(:sasl_handshake), do: 0..1
  def api_version_range(:sasl_authenticate), do: 0..1

  # API versions
  def api_version_range(:api_versions), do: 0..3

  @doc """
  Returns a unique string for use in tests.
  """
  def unique_string do
    "test-topic-#{:erlang.unique_integer([:positive])}"
  end

  def compare_binaries(lhs, rhs) do
    bytes_per_chunk = 16
    chunks_lhs = chunk_binary(lhs, bytes_per_chunk)
    chunks_rhs = chunk_binary(rhs, bytes_per_chunk)

    max_l = max(length(chunks_lhs), length(chunks_rhs))

    chunks_lhs = pad_list(chunks_lhs, max_l, nil)
    chunks_rhs = pad_list(chunks_rhs, max_l, nil)
    chunks = Enum.zip(chunks_lhs, chunks_rhs)

    chunk_compares =
      Enum.map(Enum.with_index(chunks), fn {{chunk_lhs, chunk_rhs}, ix} ->
        same =
          if chunk_lhs == chunk_rhs do
            "SAME"
          else
            "DIFF"
          end

        [
          "Bytes #{ix * bytes_per_chunk}-#{(ix + 1) * bytes_per_chunk} (#{same})",
          desc_chunk("lhs", chunk_lhs),
          desc_chunk("rhs", chunk_rhs)
        ]
      end)

    Enum.join(
      List.flatten(
        [
          "lhs size: #{byte_size(lhs)}",
          "rhs size: #{byte_size(rhs)}"
        ] ++ chunk_compares
      ),
      "\n"
    )
  end

  defp desc_chunk(head, nil), do: "#{head}: <NONE>"
  defp desc_chunk(head, chunk), do: "#{head}: #{inspect(chunk)}"

  defp chunk_binary(b, num_bytes) do
    Enum.reverse(chunk_binary(b, num_bytes, []))
  end

  defp chunk_binary(b, num_bytes, acc) when byte_size(b) <= num_bytes, do: [b | acc]

  defp chunk_binary(b, num_bytes, acc) do
    <<part::size(num_bytes)-binary, rest::bits>> = b
    chunk_binary(rest, num_bytes, [part | acc])
  end

  defp pad_list(l, n, _pad_with) when length(l) >= n, do: l

  defp pad_list(l, n, pad_with) do
    l ++ List.duplicate(pad_with, n - length(l))
  end

  @doc """
  Calls the given function up to `max_retries` times, sleeping `delay_ms` between each call.

  Retries on transient Kafka errors that are expected under chaos conditions:
  - 15: COORDINATOR_NOT_AVAILABLE
  - 16: NOT_COORDINATOR
  - 25: UNKNOWN_MEMBER_ID
  - 27: REBALANCE_IN_PROGRESS
  - 79: COORDINATOR_LOAD_IN_PROGRESS

  ## Options
  - `:max_retries` - Maximum number of retries (default: 5 for chaos tests)
  - `:delay_ms` - Delay between retries in milliseconds (default: 2000)
  - `:accept_errors` - List of error codes to accept as success (default: [])
  """
  def with_retry(fun, opts \\ []) do
    max_retries = Keyword.get(opts, :max_retries, 5)
    delay_ms = Keyword.get(opts, :delay_ms, 2000)
    accept_errors = Keyword.get(opts, :accept_errors, [])
    do_with_retry(max_retries, fun, nil, delay_ms, accept_errors)
  end

  # Transient errors that indicate "try again later"
  @transient_errors [
    15,  # COORDINATOR_NOT_AVAILABLE
    16,  # NOT_COORDINATOR
    25,  # UNKNOWN_MEMBER_ID (often transient during rebalance)
    27,  # REBALANCE_IN_PROGRESS
    79   # COORDINATOR_LOAD_IN_PROGRESS
  ]

  defp do_with_retry(0, _fun, result, _delay_ms, _accept_errors), do: result

  defp do_with_retry(n, fun, _result, delay_ms, accept_errors) do
    case fun.() do
      {:ok, response = %{error_code: 0}} ->
        {:ok, response}

      {:ok, response = %{error_code: code}} when code in @transient_errors ->
        # Transient error - retry after delay
        :timer.sleep(delay_ms)
        do_with_retry(n - 1, fun, {:ok, response}, delay_ms, accept_errors)

      {:ok, response = %{error_code: code}} ->
        # Check if this error is acceptable
        if code in accept_errors do
          {:ok, response}
        else
          :timer.sleep(delay_ms)
          do_with_retry(n - 1, fun, {:ok, response}, delay_ms, accept_errors)
        end

      {:error, _} = error ->
        :timer.sleep(delay_ms)
        do_with_retry(n - 1, fun, error, delay_ms, accept_errors)

      other ->
        :timer.sleep(delay_ms)
        do_with_retry(n - 1, fun, other, delay_ms, accept_errors)
    end
  end

  # ============================================
  # Edge Case Test Helpers
  # ============================================

  @doc """
  Truncates binary to a specified length.
  Used for testing truncated binary error handling.
  """
  @spec truncate_binary(binary(), non_neg_integer()) :: binary()
  def truncate_binary(binary, length) when is_binary(binary) and length >= 0 do
    binary_part(binary, 0, min(length, byte_size(binary)))
  end

  @doc """
  Returns multiple truncation points for a binary.
  Useful for testing various truncation scenarios.
  """
  @spec truncation_points(binary()) :: [non_neg_integer()]
  def truncation_points(binary) when is_binary(binary) do
    size = byte_size(binary)

    [
      # Empty
      0,
      # Just correlation_id prefix
      2,
      # Partial correlation_id
      3,
      # Mid-point
      div(size, 2),
      # Near end
      size - 1
    ]
    |> Enum.filter(&(&1 < size and &1 >= 0))
    |> Enum.uniq()
  end

  @doc """
  Appends extra bytes to a binary.
  Used for testing extra trailing bytes handling.
  """
  @spec append_extra_bytes(binary(), binary() | non_neg_integer()) :: binary()
  def append_extra_bytes(binary, extra) when is_binary(binary) and is_binary(extra) do
    binary <> extra
  end

  def append_extra_bytes(binary, count) when is_binary(binary) and is_integer(count) do
    binary <> :crypto.strong_rand_bytes(count)
  end

  @doc """
  Modifies the correlation_id in a response binary.
  Used for testing correlation_id mismatch handling.

  Note: Assumes standard response format with correlation_id as first 4 bytes.
  """
  @spec modify_correlation_id(binary(), non_neg_integer()) :: binary()
  def modify_correlation_id(<<_old_id::32, rest::binary>>, new_id) do
    <<new_id::32, rest::binary>>
  end

  @doc """
  Extracts the correlation_id from a response binary.
  """
  @spec extract_correlation_id(binary()) :: non_neg_integer()
  def extract_correlation_id(<<correlation_id::32, _rest::binary>>) do
    correlation_id
  end

  @doc """
  Returns the API key for a given API name.
  """
  @spec api_key(atom()) :: non_neg_integer()
  def api_key(:produce), do: 0
  def api_key(:fetch), do: 1
  def api_key(:list_offsets), do: 2
  def api_key(:metadata), do: 3
  def api_key(:offset_commit), do: 8
  def api_key(:offset_fetch), do: 9
  def api_key(:find_coordinator), do: 10
  def api_key(:join_group), do: 11
  def api_key(:heartbeat), do: 12
  def api_key(:leave_group), do: 13
  def api_key(:sync_group), do: 14
  def api_key(:describe_groups), do: 15
  def api_key(:list_groups), do: 16
  def api_key(:sasl_handshake), do: 17
  def api_key(:api_versions), do: 18
  def api_key(:create_topics), do: 19
  def api_key(:delete_topics), do: 20
  def api_key(:delete_records), do: 21
  def api_key(:init_producer_id), do: 22
  def api_key(:offset_for_leader_epoch), do: 23
  def api_key(:add_partitions_to_txn), do: 24
  def api_key(:add_offsets_to_txn), do: 25
  def api_key(:end_txn), do: 26
  def api_key(:txn_offset_commit), do: 28
  def api_key(:describe_acls), do: 29
  def api_key(:create_acls), do: 30
  def api_key(:delete_acls), do: 31
  def api_key(:describe_configs), do: 32
  def api_key(:alter_configs), do: 33
  def api_key(:alter_replica_log_dirs), do: 34
  def api_key(:describe_log_dirs), do: 35
  def api_key(:sasl_authenticate), do: 36
  def api_key(:create_partitions), do: 37
  def api_key(:create_delegation_token), do: 38
  def api_key(:renew_delegation_token), do: 39
  def api_key(:expire_delegation_token), do: 40
  def api_key(:describe_delegation_token), do: 41
  def api_key(:delete_groups), do: 42
  def api_key(:elect_leaders), do: 43
  def api_key(:incremental_alter_configs), do: 44
  def api_key(:alter_partition_reassignments), do: 45
  def api_key(:list_partition_reassignments), do: 46
  def api_key(:offset_delete), do: 47

  @doc """
  Builds a minimal valid response binary for testing.
  This creates a response with just the correlation_id and optional error_code.
  """
  @spec build_minimal_response(non_neg_integer(), keyword()) :: binary()
  def build_minimal_response(correlation_id, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 0)
    throttle_time = Keyword.get(opts, :throttle_time_ms)
    include_error = Keyword.get(opts, :include_error, true)

    base = <<correlation_id::32>>

    base =
      if throttle_time do
        base <> <<throttle_time::32>>
      else
        base
      end

    if include_error do
      base <> <<error_code::16-signed>>
    else
      base
    end
  end

  @doc """
  Asserts that deserializing a truncated binary raises an error.
  Returns :ok on success, raises on failure.
  Accepts either MatchError or FunctionClauseError since different deserializers
  may raise different exceptions depending on where the truncation occurs.
  """
  defmacro assert_truncated_error(response_module, binary, truncate_at) do
    quote do
      truncated = Kayrock.TestSupport.truncate_binary(unquote(binary), unquote(truncate_at))

      assert_raise_any([MatchError, FunctionClauseError], fn ->
        unquote(response_module).deserialize(truncated)
      end)
    end
  end

  @doc """
  Asserts that the given function raises one of the specified exception types.
  """
  def assert_raise_any(exception_types, fun) when is_list(exception_types) do
    try do
      fun.()

      raise ExUnit.AssertionError,
        message: "Expected one of #{inspect(exception_types)} but no exception was raised"
    rescue
      e ->
        exception_type = e.__struct__

        if exception_type in exception_types do
          :ok
        else
          reraise e, __STACKTRACE__
        end
    end
  end

  @doc """
  Asserts that deserializing a binary with extra bytes returns the extra bytes as rest.
  """
  defmacro assert_extra_bytes_returned(response_module, binary, extra_bytes) do
    quote do
      with_extra = unquote(binary) <> unquote(extra_bytes)
      {_response, rest} = unquote(response_module).deserialize(with_extra)
      assert rest == unquote(extra_bytes)
    end
  end

  @doc """
  Returns common Kafka error codes for testing.
  """
  def error_codes do
    %{
      none: 0,
      unknown_server_error: -1,
      offset_out_of_range: 1,
      corrupt_message: 2,
      unknown_topic_or_partition: 3,
      invalid_fetch_size: 4,
      leader_not_available: 5,
      not_leader_or_follower: 6,
      request_timed_out: 7,
      broker_not_available: 8,
      replica_not_available: 9,
      message_too_large: 10,
      stale_controller_epoch: 11,
      offset_metadata_too_large: 12,
      coordinator_not_available: 15,
      not_coordinator: 16,
      invalid_topic_exception: 17,
      record_list_too_large: 18,
      not_enough_replicas: 19,
      not_enough_replicas_after_append: 20,
      invalid_required_acks: 21,
      illegal_generation: 22,
      inconsistent_group_protocol: 23,
      invalid_group_id: 24,
      unknown_member_id: 25,
      invalid_session_timeout: 26,
      rebalance_in_progress: 27,
      invalid_commit_offset_size: 28,
      topic_authorization_failed: 29,
      group_authorization_failed: 30,
      cluster_authorization_failed: 31,
      invalid_timestamp: 32,
      unsupported_sasl_mechanism: 33,
      illegal_sasl_state: 34,
      unsupported_version: 35,
      topic_already_exists: 36,
      invalid_partitions: 37,
      invalid_replication_factor: 38,
      invalid_replica_assignment: 39,
      invalid_config: 40,
      concurrent_transactions: 51,
      transactional_id_authorization_failed: 53,
      invalid_producer_id_mapping: 59,
      fenced_leader_epoch: 74,
      unknown_leader_epoch: 75
    }
  end
end
