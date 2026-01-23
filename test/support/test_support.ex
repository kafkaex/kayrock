defmodule Kayrock.TestSupport do
  @moduledoc "Support code for tests"

  @doc """
  Returns the expected version range (min..max) for a given Kafka API.

  These ranges are based on the generated modules from kafka_protocol.
  Tests should use these ranges instead of blindly iterating over versions.
  """
  @spec api_version_range(atom()) :: Range.t()
  def api_version_range(:metadata), do: 0..9
  def api_version_range(:produce), do: 0..8
  def api_version_range(:fetch), do: 0..11
  def api_version_range(:list_offsets), do: 0..5
  def api_version_range(:offset_commit), do: 0..8
  def api_version_range(:offset_fetch), do: 0..6
  def api_version_range(:heartbeat), do: 0..4
  def api_version_range(:join_group), do: 0..6
  def api_version_range(:sync_group), do: 0..4
  def api_version_range(:leave_group), do: 0..4
  def api_version_range(:find_coordinator), do: 0..3
  def api_version_range(:describe_groups), do: 0..5
  def api_version_range(:list_groups), do: 0..3
  def api_version_range(:create_topics), do: 0..5
  def api_version_range(:delete_topics), do: 0..4
  def api_version_range(:describe_configs), do: 0..2
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
  Calls the given function up to 3 times, sleeping 1 second between each call.
  """
  def with_retry(fun), do: do_with_retry(3, fun, nil)

  defp do_with_retry(0, _fun, result), do: result

  defp do_with_retry(n, fun, _result) do
    case fun.() do
      {:ok, response = %{error_code: 0}} ->
        {:ok, response}

      result ->
        :timer.sleep(1000)
        do_with_retry(n - 1, fun, result)
    end
  end
end
