defmodule Kayrock.Serialize do
  @moduledoc """
  Serializations for primitive types for the kafka protocol
  """

  defmacro primitive_types do
    quote do
      [
        :boolean,
        :int8,
        :int16,
        :int32,
        :int64,
        :string,
        :nullable_string,
        :bytes,
        :nullable_bytes
      ]
    end
  end

  defmacro compact_primitive_types do
    quote do
      [
        :compact_string,
        :compact_nullable_string,
        :compact_bytes,
        :compact_nullable_bytes,
        :unsigned_varint,
        :varint
      ]
    end
  end

  defmacro all_primitive_types do
    quote do
      [
        :boolean,
        :int8,
        :int16,
        :int32,
        :int64,
        :string,
        :nullable_string,
        :bytes,
        :nullable_bytes,
        :compact_string,
        :compact_nullable_string,
        :compact_bytes,
        :compact_nullable_bytes,
        :unsigned_varint,
        :varint
      ]
    end
  end

  def serialize(:boolean, true), do: <<1>>
  def serialize(:boolean, _), do: <<0>>
  def serialize(:int8, val), do: <<val::8-signed>>
  def serialize(:int16, val), do: <<val::16-signed>>
  def serialize(:int32, val), do: <<val::32-signed>>
  def serialize(:int64, val), do: <<val::64-signed>>
  def serialize(:string, val), do: [<<byte_size(val)::16-signed>>, val]
  def serialize(:nullable_string, nil), do: <<-1::16-signed>>
  def serialize(:nullable_string, val), do: [<<byte_size(val)::16-signed>>, val]
  def serialize(:bytes, val), do: [<<byte_size(val)::32-signed>>, val]
  def serialize(:iodata_bytes, val), do: [<<IO.iodata_length(val)::32-signed>>, val]
  def serialize(:nullable_bytes, nil), do: <<-1::32-signed>>
  def serialize(:nullable_bytes, val), do: serialize(:bytes, val)

  # Compact types (KIP-482) - use unsigned varint, length+1 encoding
  def serialize(:compact_string, val) when is_binary(val) do
    [encode_unsigned_varint(byte_size(val) + 1), val]
  end

  def serialize(:compact_nullable_string, nil), do: encode_unsigned_varint(0)

  def serialize(:compact_nullable_string, val) when is_binary(val) do
    serialize(:compact_string, val)
  end

  def serialize(:compact_bytes, val) when is_binary(val) do
    [encode_unsigned_varint(byte_size(val) + 1), val]
  end

  def serialize(:compact_nullable_bytes, nil), do: encode_unsigned_varint(0)

  def serialize(:compact_nullable_bytes, val) when is_binary(val) do
    serialize(:compact_bytes, val)
  end

  def serialize(:unsigned_varint, val), do: encode_unsigned_varint(val)
  def serialize(:varint, val), do: Varint.LEB128.encode(Varint.Zigzag.encode(val))

  # Standard array serialization
  def serialize_array(_type, nil), do: <<-1::32-signed>>
  def serialize_array(_type, []), do: <<0::32-signed>>

  def serialize_array(type, vals) when is_list(vals) do
    [<<length(vals)::32-signed>>, Enum.map(vals, &serialize(type, &1))]
  end

  # Compact array serialization (KIP-482) - uses unsigned varint, length+1 encoding
  def serialize_compact_array(_type, nil), do: encode_unsigned_varint(0)
  def serialize_compact_array(_type, []), do: encode_unsigned_varint(1)

  def serialize_compact_array(type, vals) when is_list(vals) and is_atom(type) do
    [encode_unsigned_varint(length(vals) + 1), Enum.map(vals, &serialize(type, &1))]
  end

  def serialize_compact_array(vals, serializer_fn)
      when is_list(vals) and is_function(serializer_fn, 1) do
    [encode_unsigned_varint(length(vals) + 1), Enum.map(vals, serializer_fn)]
  end

  # Tagged fields serialization (KIP-482)
  def serialize_tagged_fields(nil), do: encode_unsigned_varint(0)
  def serialize_tagged_fields([]), do: encode_unsigned_varint(0)

  def serialize_tagged_fields(tagged_fields) when is_list(tagged_fields) do
    [
      encode_unsigned_varint(length(tagged_fields)),
      Enum.map(tagged_fields, fn {tag, data} ->
        [encode_unsigned_varint(tag), encode_unsigned_varint(byte_size(data)), data]
      end)
    ]
  end

  def encode_unsigned_varint(val) when val < 128, do: <<val>>
  def encode_unsigned_varint(val), do: encode_unsigned_varint_loop(val, [])

  defp encode_unsigned_varint_loop(val, acc) when val < 128 do
    :erlang.list_to_binary(Enum.reverse([val | acc]))
  end

  defp encode_unsigned_varint_loop(val, acc) do
    byte = Bitwise.bor(Bitwise.band(val, 0x7F), 0x80)
    encode_unsigned_varint_loop(Bitwise.bsr(val, 7), [byte | acc])
  end
end
