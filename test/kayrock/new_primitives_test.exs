defmodule Kayrock.NewPrimitivesTest do
  @moduledoc """
  Tests for the primitive types introduced for the JSON-spec-driven generator:
  `uuid`, `float64`, `uint16` and compact record sets.
  """
  use ExUnit.Case, async: true

  require Kayrock.Deserialize
  require Kayrock.Serialize

  alias Kayrock.Deserialize
  alias Kayrock.Serialize

  defp bin(iodata), do: IO.iodata_to_binary(iodata)

  describe "uuid" do
    test "serializes a 16-byte binary as-is" do
      uuid = :crypto.strong_rand_bytes(16)
      assert bin(Serialize.serialize(:uuid, uuid)) == uuid
    end

    test "nil serializes as Kafka's zero (null) UUID" do
      assert bin(Serialize.serialize(:uuid, nil)) == <<0::128>>
    end

    test "rejects binaries that are not 16 bytes" do
      assert_raise ArgumentError, ~r/16-byte binary/, fn ->
        Serialize.serialize(:uuid, <<1, 2, 3>>)
      end

      assert_raise ArgumentError, ~r/16-byte binary/, fn ->
        Serialize.serialize(:uuid, "6ba7b810-9dad-11d1-80b4-00c04fd430c8")
      end
    end

    test "deserializes 16 raw bytes" do
      uuid = :crypto.strong_rand_bytes(16)
      assert Deserialize.deserialize(:uuid, uuid <> <<9>>) == {uuid, <<9>>}
    end

    test "the zero UUID deserializes as 16 zero bytes (not nil)" do
      assert Deserialize.deserialize(:uuid, <<0::128>>) == {<<0::128>>, <<>>}
    end

    test "round-trips" do
      for uuid <- [<<0::128>>, <<-1::128>>, :crypto.strong_rand_bytes(16)] do
        assert {^uuid, <<>>} =
                 Deserialize.deserialize(:uuid, bin(Serialize.serialize(:uuid, uuid)))
      end
    end

    test "fails on truncated data" do
      assert_raise FunctionClauseError, fn -> Deserialize.deserialize(:uuid, <<1::120>>) end
    end
  end

  describe "uint16" do
    test "serializes big-endian unsigned" do
      assert bin(Serialize.serialize(:uint16, 0)) == <<0, 0>>
      assert bin(Serialize.serialize(:uint16, 9092)) == <<0x23, 0x84>>
      assert bin(Serialize.serialize(:uint16, 65_535)) == <<0xFF, 0xFF>>
    end

    test "rejects out-of-range values" do
      for bad <- [-1, 65_536, nil, 1.0] do
        assert_raise ArgumentError, ~r/uint16/, fn -> Serialize.serialize(:uint16, bad) end
      end
    end

    test "deserializes unsigned (values above int16 max stay positive)" do
      assert Deserialize.deserialize(:uint16, <<0xFF, 0xFF, 1>>) == {65_535, <<1>>}
      assert Deserialize.deserialize(:uint16, <<0x80, 0x00>>) == {32_768, <<>>}
    end

    test "round-trips" do
      for v <- [0, 1, 32_767, 32_768, 65_535] do
        assert {^v, <<>>} = Deserialize.deserialize(:uint16, bin(Serialize.serialize(:uint16, v)))
      end
    end
  end

  describe "float64" do
    test "serializes IEEE 754 binary64 big-endian" do
      assert bin(Serialize.serialize(:float64, 1.5)) == <<0x3F, 0xF8, 0::48>>
      assert bin(Serialize.serialize(:float64, 0.0)) == <<0::64>>
    end

    test "integers are widened to floats" do
      assert bin(Serialize.serialize(:float64, 2)) == bin(Serialize.serialize(:float64, 2.0))
    end

    test "raw 8-byte binaries pass through (NaN / Infinity)" do
      pos_inf = <<0x7FF0::16, 0::48>>
      assert bin(Serialize.serialize(:float64, pos_inf)) == pos_inf
    end

    test "deserializes finite values as floats" do
      assert Deserialize.deserialize(:float64, <<1.5::64-float, 7>>) == {1.5, <<7>>}
      assert {-0.0, <<>>} = Deserialize.deserialize(:float64, <<-0.0::64-float>>)
    end

    test "NaN and infinities deserialize as their raw 8 bytes" do
      pos_inf = <<0x7FF0::16, 0::48>>
      neg_inf = <<0xFFF0::16, 0::48>>
      nan = <<0x7FF8::16, 0::48>>

      for raw <- [pos_inf, neg_inf, nan] do
        assert Deserialize.deserialize(:float64, raw <> <<1>>) == {raw, <<1>>}
      end
    end

    test "round-trips" do
      for v <- [0.0, 1.0, -1.5, 1.0e300, -1.0e-300, 5.0e-324, 1.797_693_134_862_315_7e308] do
        assert {^v, <<>>} =
                 Deserialize.deserialize(:float64, bin(Serialize.serialize(:float64, v)))
      end
    end
  end

  describe "fixed-width types in arrays" do
    test "compact arrays of uuids round-trip" do
      uuids = [<<1::128>>, <<2::128>>]
      wire = bin(Serialize.serialize_compact_array(:uuid, uuids))
      assert wire == <<3, 1::128, 2::128>>
      {decoded, <<>>} = Deserialize.deserialize_compact_array(:uuid, wire)
      assert Enum.reverse(decoded) == uuids
    end

    test "standard arrays of uint16 round-trip" do
      wire = bin(Serialize.serialize_array(:uint16, [1, 65_535]))
      assert wire == <<2::32, 1::16, 0xFF, 0xFF>>
      {decoded, <<>>} = Deserialize.deserialize_array(:uint16, wire)
      assert Enum.reverse(decoded) == [1, 65_535]
    end

    test "fixed_width_types/0 covers the new primitives" do
      for t <- [:uuid, :uint16, :float64], do: assert(t in Serialize.fixed_width_types())
      assert Serialize.fixed_width_types() == Deserialize.fixed_width_types()
      for t <- [:uuid, :uint16, :float64], do: assert(t in Serialize.primitive_types())
      for t <- [:uuid, :uint16, :float64], do: assert(t in Deserialize.primitive_types())
    end
  end

  describe "compact records" do
    test "nil serializes as varint 0 (null)" do
      assert bin(Serialize.serialize_compact_records(nil)) == <<0>>
    end

    test "a record batch gets an unsigned-varint length+1 prefix" do
      batch = %Kayrock.RecordBatch{
        records: [%Kayrock.RecordBatch.Record{key: "k", value: "v", timestamp: 1, offset: 0}]
      }

      <<size::32, data::binary>> = bin(Kayrock.RecordBatch.serialize(batch))
      wire = bin(Serialize.serialize_compact_records(batch))

      {len_plus_one, rest} = Deserialize.decode_unsigned_varint(wire)
      assert len_plus_one == size + 1
      assert rest == data
    end
  end
end
