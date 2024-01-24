defmodule Kayrock.TestSupport do
  @moduledoc "Support code for tests"

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
end
