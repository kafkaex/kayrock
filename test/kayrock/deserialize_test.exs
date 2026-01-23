defmodule Kayrock.DeserializeTest do
  @moduledoc """
  Tests for Kayrock.Deserialize module - primitive type deserialization.
  """
  use ExUnit.Case, async: true

  alias Kayrock.Deserialize

  describe "boolean deserialization" do
    test "deserializes true (1)" do
      assert {1, <<>>} = Deserialize.deserialize(:boolean, <<1>>)
    end

    test "deserializes false (0)" do
      assert {0, <<>>} = Deserialize.deserialize(:boolean, <<0>>)
    end

    test "preserves remaining bytes" do
      assert {1, <<2, 3>>} = Deserialize.deserialize(:boolean, <<1, 2, 3>>)
    end
  end

  describe "int8 deserialization" do
    test "deserializes positive int8" do
      assert {127, <<>>} = Deserialize.deserialize(:int8, <<127>>)
    end

    test "deserializes negative int8" do
      assert {-1, <<>>} = Deserialize.deserialize(:int8, <<255>>)
      assert {-128, <<>>} = Deserialize.deserialize(:int8, <<128>>)
    end

    test "deserializes zero" do
      assert {0, <<>>} = Deserialize.deserialize(:int8, <<0>>)
    end
  end

  describe "int16 deserialization" do
    test "deserializes positive int16" do
      assert {256, <<>>} = Deserialize.deserialize(:int16, <<1, 0>>)
      assert {32_767, <<>>} = Deserialize.deserialize(:int16, <<127, 255>>)
    end

    test "deserializes negative int16" do
      assert {-1, <<>>} = Deserialize.deserialize(:int16, <<255, 255>>)
    end

    test "preserves remaining bytes" do
      assert {1, <<2, 3>>} = Deserialize.deserialize(:int16, <<0, 1, 2, 3>>)
    end
  end

  describe "int32 deserialization" do
    test "deserializes positive int32" do
      assert {1, <<>>} = Deserialize.deserialize(:int32, <<0, 0, 0, 1>>)
      assert {256, <<>>} = Deserialize.deserialize(:int32, <<0, 0, 1, 0>>)
    end

    test "deserializes negative int32" do
      assert {-1, <<>>} = Deserialize.deserialize(:int32, <<255, 255, 255, 255>>)
    end
  end

  describe "int64 deserialization" do
    test "deserializes positive int64" do
      assert {1, <<>>} = Deserialize.deserialize(:int64, <<0, 0, 0, 0, 0, 0, 0, 1>>)
    end

    test "deserializes negative int64" do
      assert {-1, <<>>} =
               Deserialize.deserialize(:int64, <<255, 255, 255, 255, 255, 255, 255, 255>>)
    end

    test "deserializes large positive int64" do
      assert {1_000_000_000_000, <<>>} =
               Deserialize.deserialize(:int64, <<0, 0, 0, 232, 212, 165, 16, 0>>)
    end
  end

  describe "string deserialization" do
    test "deserializes empty string" do
      assert {"", <<>>} = Deserialize.deserialize(:string, <<0, 0>>)
    end

    test "deserializes short string" do
      assert {"hello", <<>>} = Deserialize.deserialize(:string, <<0, 5, "hello">>)
    end

    test "preserves remaining bytes" do
      assert {"hi", <<1, 2>>} = Deserialize.deserialize(:string, <<0, 2, "hi", 1, 2>>)
    end
  end

  describe "nullable_string deserialization" do
    test "deserializes null string (-1 length)" do
      assert {nil, <<>>} = Deserialize.deserialize(:nullable_string, <<255, 255>>)
    end

    test "deserializes non-null string" do
      assert {"test", <<>>} = Deserialize.deserialize(:nullable_string, <<0, 4, "test">>)
    end

    test "deserializes empty string" do
      assert {"", <<>>} = Deserialize.deserialize(:nullable_string, <<0, 0>>)
    end
  end

  describe "bytes deserialization" do
    test "deserializes empty bytes" do
      assert {"", <<>>} = Deserialize.deserialize(:bytes, <<0, 0, 0, 0>>)
    end

    test "deserializes bytes" do
      assert {<<1, 2, 3>>, <<>>} = Deserialize.deserialize(:bytes, <<0, 0, 0, 3, 1, 2, 3>>)
    end
  end

  describe "nullable_bytes deserialization" do
    test "deserializes null bytes (-1 length)" do
      assert {nil, <<>>} = Deserialize.deserialize(:nullable_bytes, <<255, 255, 255, 255>>)
    end

    test "deserializes non-null bytes" do
      assert {<<1, 2>>, <<>>} = Deserialize.deserialize(:nullable_bytes, <<0, 0, 0, 2, 1, 2>>)
    end
  end

  describe "compact_string deserialization" do
    test "deserializes null compact string (length 0)" do
      assert {nil, <<>>} = Deserialize.deserialize(:compact_string, <<0>>)
    end

    test "deserializes empty compact string (length 1)" do
      assert {"", <<>>} = Deserialize.deserialize(:compact_string, <<1>>)
    end

    test "deserializes compact string" do
      # length_plus_one = 6, so actual length = 5
      assert {"hello", <<>>} = Deserialize.deserialize(:compact_string, <<6, "hello">>)
    end

    test "preserves remaining bytes" do
      assert {"hi", <<1, 2>>} = Deserialize.deserialize(:compact_string, <<3, "hi", 1, 2>>)
    end
  end

  describe "compact_nullable_string deserialization" do
    test "delegates to compact_string" do
      assert {nil, <<>>} = Deserialize.deserialize(:compact_nullable_string, <<0>>)
      assert {"test", <<>>} = Deserialize.deserialize(:compact_nullable_string, <<5, "test">>)
    end
  end

  describe "compact_bytes deserialization" do
    test "deserializes null compact bytes" do
      assert {nil, <<>>} = Deserialize.deserialize(:compact_bytes, <<0>>)
    end

    test "deserializes empty compact bytes" do
      assert {"", <<>>} = Deserialize.deserialize(:compact_bytes, <<1>>)
    end

    test "deserializes compact bytes" do
      assert {<<1, 2, 3>>, <<>>} = Deserialize.deserialize(:compact_bytes, <<4, 1, 2, 3>>)
    end
  end

  describe "compact_nullable_bytes deserialization" do
    test "delegates to compact_bytes" do
      assert {nil, <<>>} = Deserialize.deserialize(:compact_nullable_bytes, <<0>>)
    end
  end

  describe "unsigned_varint deserialization" do
    test "deserializes single byte varint" do
      assert {0, <<>>} = Deserialize.deserialize(:unsigned_varint, <<0>>)
      assert {1, <<>>} = Deserialize.deserialize(:unsigned_varint, <<1>>)
      assert {127, <<>>} = Deserialize.deserialize(:unsigned_varint, <<127>>)
    end

    test "deserializes multi-byte varint" do
      # 128 = 0x80 0x01
      assert {128, <<>>} = Deserialize.deserialize(:unsigned_varint, <<128, 1>>)
      # 300 = 0xAC 0x02
      assert {300, <<>>} = Deserialize.deserialize(:unsigned_varint, <<172, 2>>)
    end

    test "preserves remaining bytes" do
      assert {1, <<2, 3>>} = Deserialize.deserialize(:unsigned_varint, <<1, 2, 3>>)
    end
  end

  describe "varint (signed zigzag) deserialization" do
    test "deserializes zero" do
      assert {0, <<>>} = Deserialize.deserialize(:varint, <<0>>)
    end

    test "deserializes positive values" do
      # zigzag: 1 -> 2
      assert {1, <<>>} = Deserialize.deserialize(:varint, <<2>>)
    end

    test "deserializes negative values" do
      # zigzag: -1 -> 1
      assert {-1, <<>>} = Deserialize.deserialize(:varint, <<1>>)
    end
  end

  describe "deserialize_array" do
    test "deserializes empty array" do
      assert {[], <<>>} = Deserialize.deserialize_array(:int32, <<0, 0, 0, 0>>)
    end

    test "deserializes array of int32" do
      data = <<0, 0, 0, 2, 0, 0, 0, 1, 0, 0, 0, 2>>
      {result, <<>>} = Deserialize.deserialize_array(:int32, data)
      assert Enum.sort(result) == [1, 2]
    end

    test "deserializes array of strings" do
      data = <<0, 0, 0, 2, 0, 1, "a", 0, 1, "b">>
      {result, <<>>} = Deserialize.deserialize_array(:string, data)
      assert Enum.sort(result) == ["a", "b"]
    end
  end

  describe "deserialize_compact_array" do
    test "deserializes null compact array (0)" do
      assert {nil, <<>>} = Deserialize.deserialize_compact_array(:int32, <<0>>)
    end

    test "deserializes empty compact array (1)" do
      assert {[], <<>>} = Deserialize.deserialize_compact_array(:int32, <<1>>)
    end

    test "deserializes compact array with elements" do
      # length_plus_one = 3, so 2 elements
      data = <<3, 0, 0, 0, 1, 0, 0, 0, 2>>
      {result, <<>>} = Deserialize.deserialize_compact_array(:int32, data)
      assert Enum.sort(result) == [1, 2]
    end

    test "deserializes compact array with deserializer function" do
      deserializer = fn <<val::32-signed, rest::binary>> -> {val, rest} end
      data = <<3, 0, 0, 0, 10, 0, 0, 0, 20>>
      {result, <<>>} = Deserialize.deserialize_compact_array(data, deserializer)
      assert Enum.sort(result) == [10, 20]
    end
  end

  describe "deserialize_tagged_fields" do
    test "deserializes empty tagged fields" do
      assert {[], <<>>} = Deserialize.deserialize_tagged_fields(<<0>>)
    end

    test "deserializes single tagged field" do
      # 1 tag, tag=0, size=2, data=<<1,2>>
      data = <<1, 0, 2, 1, 2>>
      {result, <<>>} = Deserialize.deserialize_tagged_fields(data)
      assert result == [{0, <<1, 2>>}]
    end

    test "deserializes multiple tagged fields" do
      # 2 tags: tag=0 size=1 data=<<1>>, tag=1 size=2 data=<<2,3>>
      data = <<2, 0, 1, 1, 1, 2, 2, 3>>
      {result, <<>>} = Deserialize.deserialize_tagged_fields(data)
      assert length(result) == 2
    end
  end

  describe "decode_unsigned_varint" do
    test "decodes single byte values" do
      assert {0, <<>>} = Deserialize.decode_unsigned_varint(<<0>>)
      assert {1, <<>>} = Deserialize.decode_unsigned_varint(<<1>>)
      assert {127, <<>>} = Deserialize.decode_unsigned_varint(<<127>>)
    end

    test "decodes multi-byte values" do
      assert {128, <<>>} = Deserialize.decode_unsigned_varint(<<128, 1>>)
      assert {16_384, <<>>} = Deserialize.decode_unsigned_varint(<<128, 128, 1>>)
    end
  end
end
