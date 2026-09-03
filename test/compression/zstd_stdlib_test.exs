defmodule Kayrock.Compression.ZstdStdlibTest do
  @moduledoc """
  Pins the OTP-stdlib `:zstd` branch of `Kayrock.Compression.Zstd`.

  `Kayrock.Compression.Zstd` has two backends and picks between them with
  `function_exported?(:zstd, :compress, 2)`. That is false for a module that is
  merely not loaded yet, so the branch taken depends on code-loading mode:

    * `:interactive` (dev, test, iex) leaves `:zstd` lazily unloaded, so the
      `:ezstd` backend is used;
    * `:embedded` (any `mix release`, i.e. production) preloads every module,
      so the OTP-stdlib backend is used.

  Every other test in this suite therefore only ever exercises `:ezstd`, which
  is how two stdlib-only bugs shipped: OTP's `zstd:compress/2` takes an options
  map (a bare integer level raises `FunctionClauseError`), and both
  `zstd:compress/2` and `zstd:decompress/1` return **iolists** where callers
  pattern-match on binaries — the record-batch decoder fails with
  `ArgumentError: not a valid LEB128 encoded integer` on an iolist.

  These tests force `:zstd` to be loaded so the stdlib branch is the one under
  test. Loading is process-global and permanent for the VM run, hence
  `async: false`; it is harmless because a correct implementation makes both
  backends observationally identical.
  """
  use ExUnit.Case, async: false

  alias Kayrock.Compression
  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record

  @zstd_attribute 4

  @data String.duplicate("moderate_action payload ", 200)

  setup_all do
    # OTP < 27 has no :zstd module; nothing to pin there.
    case Code.ensure_loaded(:zstd) do
      {:module, :zstd} -> :ok
      {:error, _} -> :ok
    end
  end

  describe "OTP stdlib backend" do
    @describetag :stdlib_zstd

    test "compress/2 returns a binary, not an iolist" do
      with_stdlib_zstd(fn ->
        {compressed, @zstd_attribute} = Compression.compress(:zstd, @data)

        assert is_binary(compressed),
               "stdlib zstd:compress/2 returns an iolist; it must be normalised to a binary"
      end)
    end

    test "compress/3 honours the level rather than raising" do
      with_stdlib_zstd(fn ->
        # OTP wants %{compressionLevel: n}; passing the integer straight through
        # raises FunctionClauseError.
        {low, _} = Compression.compress(:zstd, @data, level: 1)
        {high, _} = Compression.compress(:zstd, @data, level: 22)

        assert is_binary(low) and is_binary(high)
        assert byte_size(high) <= byte_size(low)
      end)
    end

    test "decompress/2 returns a binary, not an iolist" do
      with_stdlib_zstd(fn ->
        {compressed, _} = Compression.compress(:zstd, @data)
        decompressed = Compression.decompress(@zstd_attribute, compressed)

        assert is_binary(decompressed),
               "stdlib zstd:decompress/1 returns an iolist; it must be normalised to a binary"

        assert decompressed == @data
      end)
    end

    test "decompresses frames written by :ezstd" do
      # The production shape: someone else compressed (Kafka Connect, librdkafka,
      # an :ezstd-backed producer), this node decompresses on the stdlib branch.
      with_stdlib_zstd(fn ->
        if Code.ensure_loaded?(:ezstd) do
          foreign = :ezstd.compress(@data, 3)

          assert Compression.decompress(@zstd_attribute, foreign) == @data
        end
      end)
    end

    test "writes frames :ezstd can read back" do
      with_stdlib_zstd(fn ->
        if Code.ensure_loaded?(:ezstd) do
          {ours, _} = Compression.compress(:zstd, @data)

          assert :ezstd.decompress(ours) == @data
        end
      end)
    end

    test "a zstd record batch round-trips through the record decoder" do
      # The end-to-end failure: an iolist reaches decode_varint and raises
      # ArgumentError, which kafka_ex reports only as :parse_error.
      with_stdlib_zstd(fn ->
        batch = %RecordBatch{
          attributes: @zstd_attribute,
          records: [%Record{key: "k", value: @data, headers: []}]
        }

        <<_size::32-signed, blob::binary>> =
          batch |> RecordBatch.serialize() |> IO.iodata_to_binary()

        assert [%RecordBatch{records: [record]}] = RecordBatch.deserialize(blob)
        assert record.value == @data
      end)
    end
  end

  # Runs `fun` only when the stdlib backend is the one that will be selected,
  # so the suite still passes on OTP releases without :zstd.
  defp with_stdlib_zstd(fun) do
    if function_exported?(:zstd, :compress, 2) do
      fun.()
    else
      :ok
    end
  end
end
