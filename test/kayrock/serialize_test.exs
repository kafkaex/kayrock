defmodule Kayrock.SerializeTest do
  @moduledoc """
  Tests for Kayrock.Serialize module - primitive type serialization.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Serialize

  describe "boolean serialization" do
    test "serializes true as <<1>>" do
      assert <<1>> = Serialize.serialize(:boolean, true)
    end

    test "serializes false as <<0>>" do
      assert <<0>> = Serialize.serialize(:boolean, false)
    end

    test "serializes nil as <<0>>" do
      assert <<0>> = Serialize.serialize(:boolean, nil)
    end
  end

  describe "int8 serialization" do
    test "serializes positive int8" do
      assert <<127>> = Serialize.serialize(:int8, 127)
    end

    test "serializes negative int8" do
      assert <<255>> = Serialize.serialize(:int8, -1)
      assert <<128>> = Serialize.serialize(:int8, -128)
    end

    test "serializes zero" do
      assert <<0>> = Serialize.serialize(:int8, 0)
    end
  end

  describe "int16 serialization" do
    test "serializes positive int16" do
      assert <<0, 1>> = Serialize.serialize(:int16, 1)
      assert <<1, 0>> = Serialize.serialize(:int16, 256)
      assert <<127, 255>> = Serialize.serialize(:int16, 32767)
    end

    test "serializes negative int16" do
      assert <<255, 255>> = Serialize.serialize(:int16, -1)
    end
  end

  describe "int32 serialization" do
    test "serializes positive int32" do
      assert <<0, 0, 0, 1>> = Serialize.serialize(:int32, 1)
      assert <<0, 0, 1, 0>> = Serialize.serialize(:int32, 256)
    end

    test "serializes negative int32" do
      assert <<255, 255, 255, 255>> = Serialize.serialize(:int32, -1)
    end
  end

  describe "int64 serialization" do
    test "serializes positive int64" do
      assert <<0, 0, 0, 0, 0, 0, 0, 1>> = Serialize.serialize(:int64, 1)
    end

    test "serializes negative int64" do
      assert <<255, 255, 255, 255, 255, 255, 255, 255>> = Serialize.serialize(:int64, -1)
    end

    test "serializes large positive int64" do
      assert <<0, 0, 0, 232, 212, 165, 16, 0>> =
               Serialize.serialize(:int64, 1_000_000_000_000)
    end
  end

  describe "string serialization" do
    test "serializes empty string" do
      assert [<<0, 0>>, ""] = Serialize.serialize(:string, "")
    end

    test "serializes short string" do
      assert [<<0, 5>>, "hello"] = Serialize.serialize(:string, "hello")
    end
  end

  describe "nullable_string serialization" do
    test "serializes null string" do
      assert <<255, 255>> = Serialize.serialize(:nullable_string, nil)
    end

    test "serializes non-null string" do
      assert [<<0, 4>>, "test"] = Serialize.serialize(:nullable_string, "test")
    end

    test "serializes empty string" do
      assert [<<0, 0>>, ""] = Serialize.serialize(:nullable_string, "")
    end
  end

  describe "bytes serialization" do
    test "serializes empty bytes" do
      assert [<<0, 0, 0, 0>>, ""] = Serialize.serialize(:bytes, "")
    end

    test "serializes bytes" do
      assert [<<0, 0, 0, 3>>, <<1, 2, 3>>] = Serialize.serialize(:bytes, <<1, 2, 3>>)
    end
  end

  describe "iodata_bytes serialization" do
    test "serializes iolist" do
      assert [<<0, 0, 0, 3>>, ["a", "b", "c"]] =
               Serialize.serialize(:iodata_bytes, ["a", "b", "c"])
    end

    test "serializes binary" do
      assert [<<0, 0, 0, 3>>, "abc"] = Serialize.serialize(:iodata_bytes, "abc")
    end
  end

  describe "nullable_bytes serialization" do
    test "serializes null bytes" do
      assert <<255, 255, 255, 255>> = Serialize.serialize(:nullable_bytes, nil)
    end

    test "serializes non-null bytes" do
      assert [<<0, 0, 0, 2>>, <<1, 2>>] = Serialize.serialize(:nullable_bytes, <<1, 2>>)
    end
  end

  describe "compact_string serialization" do
    test "serializes empty string (length_plus_one = 1)" do
      assert [<<1>>, ""] = Serialize.serialize(:compact_string, "")
    end

    test "serializes string (length_plus_one)" do
      # "hello" = 5 bytes, length_plus_one = 6
      assert [<<6>>, "hello"] = Serialize.serialize(:compact_string, "hello")
    end
  end

  describe "compact_nullable_string serialization" do
    test "serializes null (length_plus_one = 0)" do
      assert <<0>> = Serialize.serialize(:compact_nullable_string, nil)
    end

    test "serializes non-null string" do
      # "test" = 4 bytes, length_plus_one = 5
      assert [<<5>>, "test"] = Serialize.serialize(:compact_nullable_string, "test")
    end
  end

  describe "compact_bytes serialization" do
    test "serializes empty bytes (length_plus_one = 1)" do
      assert [<<1>>, ""] = Serialize.serialize(:compact_bytes, "")
    end

    test "serializes bytes" do
      assert [<<4>>, <<1, 2, 3>>] = Serialize.serialize(:compact_bytes, <<1, 2, 3>>)
    end
  end

  describe "compact_nullable_bytes serialization" do
    test "serializes null (length_plus_one = 0)" do
      assert <<0>> = Serialize.serialize(:compact_nullable_bytes, nil)
    end

    test "serializes non-null bytes" do
      assert [<<3>>, <<1, 2>>] = Serialize.serialize(:compact_nullable_bytes, <<1, 2>>)
    end
  end

  describe "unsigned_varint serialization" do
    test "serializes single-byte values" do
      assert <<0>> = Serialize.serialize(:unsigned_varint, 0)
      assert <<1>> = Serialize.serialize(:unsigned_varint, 1)
      assert <<127>> = Serialize.serialize(:unsigned_varint, 127)
    end

    test "serializes multi-byte values" do
      # 128 = 0x80 0x01
      assert <<128, 1>> = Serialize.serialize(:unsigned_varint, 128)
      # 300 = 0xAC 0x02
      assert <<172, 2>> = Serialize.serialize(:unsigned_varint, 300)
    end
  end

  describe "varint (signed zigzag) serialization" do
    test "serializes zero" do
      assert Serialize.serialize(:varint, 0) |> IO.iodata_to_binary() == <<0>>
    end

    test "serializes positive values" do
      # zigzag: 1 -> 2
      assert Serialize.serialize(:varint, 1) |> IO.iodata_to_binary() == <<2>>
    end

    test "serializes negative values" do
      # zigzag: -1 -> 1
      assert Serialize.serialize(:varint, -1) |> IO.iodata_to_binary() == <<1>>
    end
  end

  describe "serialize_array" do
    test "serializes null array" do
      assert <<255, 255, 255, 255>> = Serialize.serialize_array(:int32, nil)
    end

    test "serializes empty array" do
      assert <<0, 0, 0, 0>> = Serialize.serialize_array(:int32, [])
    end

    test "serializes array of int32" do
      result = Serialize.serialize_array(:int32, [1, 2])
      assert IO.iodata_to_binary(result) == <<0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2>>
    end

    test "serializes array of strings" do
      result = Serialize.serialize_array(:string, ["a", "b"])
      assert IO.iodata_to_binary(result) == <<0, 0, 0, 2, 0, 1, "a", 0, 1, "b">>
    end
  end

  describe "serialize_compact_array" do
    test "serializes null compact array (0)" do
      assert <<0>> = Serialize.serialize_compact_array(:int32, nil)
    end

    test "serializes empty compact array (1)" do
      assert <<1>> = Serialize.serialize_compact_array(:int32, [])
    end

    test "serializes compact array with elements" do
      # length_plus_one = 3 for 2 elements
      result = Serialize.serialize_compact_array(:int32, [1, 2])
      assert IO.iodata_to_binary(result) == <<3, 0, 0, 0, 1, 0, 0, 0, 2>>
    end

    test "serializes compact array with serializer function" do
      serializer = fn val -> <<val::32-signed>> end
      result = Serialize.serialize_compact_array([10, 20], serializer)
      assert IO.iodata_to_binary(result) == <<3, 0, 0, 0, 10, 0, 0, 0, 20>>
    end
  end

  describe "serialize_tagged_fields" do
    test "serializes nil tagged fields" do
      assert <<0>> = Serialize.serialize_tagged_fields(nil)
    end

    test "serializes empty tagged fields" do
      assert <<0>> = Serialize.serialize_tagged_fields([])
    end

    test "serializes single tagged field" do
      # 1 tag, tag=0, size=2, data=<<1,2>>
      result = Serialize.serialize_tagged_fields([{0, <<1, 2>>}])
      assert IO.iodata_to_binary(result) == <<1, 0, 2, 1, 2>>
    end

    test "serializes multiple tagged fields" do
      result = Serialize.serialize_tagged_fields([{0, <<1>>}, {1, <<2, 3>>}])
      # 2 tags, tag=0 size=1 data=<<1>>, tag=1 size=2 data=<<2,3>>
      assert IO.iodata_to_binary(result) == <<2, 0, 1, 1, 1, 2, 2, 3>>
    end
  end

  describe "encode_unsigned_varint" do
    test "encodes single-byte values" do
      assert <<0>> = Serialize.encode_unsigned_varint(0)
      assert <<1>> = Serialize.encode_unsigned_varint(1)
      assert <<127>> = Serialize.encode_unsigned_varint(127)
    end

    test "encodes multi-byte values" do
      assert <<128, 1>> = Serialize.encode_unsigned_varint(128)
      assert <<128, 128, 1>> = Serialize.encode_unsigned_varint(16384)
    end

    test "encodes large values" do
      # Test a larger value
      result = Serialize.encode_unsigned_varint(268_435_455)
      # 268435455 = 0xFF 0xFF 0xFF 0x7F
      assert <<255, 255, 255, 127>> = result
    end
  end

  describe "round-trip with Deserialize" do
    alias Kayrock.Deserialize

    test "int32 round-trip" do
      value = 12345
      serialized = Serialize.serialize(:int32, value)
      {deserialized, <<>>} = Deserialize.deserialize(:int32, IO.iodata_to_binary(serialized))
      assert deserialized == value
    end

    test "string round-trip" do
      value = "test string"
      serialized = Serialize.serialize(:string, value)
      {deserialized, <<>>} = Deserialize.deserialize(:string, IO.iodata_to_binary(serialized))
      assert deserialized == value
    end

    test "nullable_string round-trip with nil" do
      serialized = Serialize.serialize(:nullable_string, nil)

      {deserialized, <<>>} =
        Deserialize.deserialize(:nullable_string, IO.iodata_to_binary(serialized))

      assert deserialized == nil
    end

    test "compact_string round-trip" do
      value = "compact test"
      serialized = Serialize.serialize(:compact_string, value)

      {deserialized, <<>>} =
        Deserialize.deserialize(:compact_string, IO.iodata_to_binary(serialized))

      assert deserialized == value
    end

    test "unsigned_varint round-trip" do
      for value <- [0, 1, 127, 128, 300, 16384, 268_435_455] do
        serialized = Serialize.serialize(:unsigned_varint, value)

        {deserialized, <<>>} =
          Deserialize.deserialize(:unsigned_varint, IO.iodata_to_binary(serialized))

        assert deserialized == value, "Failed for value #{value}"
      end
    end
  end
end
