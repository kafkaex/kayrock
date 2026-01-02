defmodule Kayrock.Deserialize do
  @moduledoc """
  Deserialization for primitive types for the kafka protocol
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

  def deserialize(:boolean, <<val, rest::binary>>), do: {val, rest}
  def deserialize(:int8, <<val::8-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int16, <<val::16-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int32, <<val::32-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int64, <<val::64-signed, rest::binary>>), do: {val, rest}

  def deserialize(:string, <<len::16-signed, val::size(len)-binary, rest::binary>>),
    do: {val, rest}

  def deserialize(:nullable_string, <<-1::16-signed, rest::binary>>), do: {nil, rest}
  def deserialize(:nullable_string, data), do: deserialize(:string, data)

  def deserialize(:bytes, <<len::32-signed, val::size(len)-binary, rest::binary>>),
    do: {val, rest}

  def deserialize(:nullable_bytes, <<-1::32-signed, rest::binary>>), do: {nil, rest}
  def deserialize(:nullable_bytes, data), do: deserialize(:bytes, data)

  # Compact types (KIP-482)
  def deserialize(:compact_string, data) do
    {len_plus_one, rest} = decode_unsigned_varint(data)

    case len_plus_one do
      0 ->
        {nil, rest}

      n ->
        len = n - 1
        <<val::size(len)-binary, rest2::binary>> = rest
        {val, rest2}
    end
  end

  def deserialize(:compact_nullable_string, data), do: deserialize(:compact_string, data)

  def deserialize(:compact_bytes, data) do
    {len_plus_one, rest} = decode_unsigned_varint(data)

    case len_plus_one do
      0 ->
        {nil, rest}

      n ->
        len = n - 1
        <<val::size(len)-binary, rest2::binary>> = rest
        {val, rest2}
    end
  end

  def deserialize(:compact_nullable_bytes, data), do: deserialize(:compact_bytes, data)

  def deserialize(:unsigned_varint, data), do: decode_unsigned_varint(data)

  def deserialize(:varint, data) do
    {encoded, rest} = decode_unsigned_varint(data)
    {Varint.Zigzag.decode(encoded), rest}
  end

  def deserialize_array(_type, <<0::32-signed, rest::bits>>), do: {[], rest}

  def deserialize_array(type, <<len::32-signed, rest::bits>>) do
    Enum.reduce(1..len, {[], rest}, fn _ix, {acc, d} ->
      {val, r} = deserialize(type, d)
      {[val | acc], r}
    end)
  end

  # Compact array (KIP-482)
  def deserialize_compact_array(type, data) when is_atom(type) do
    {len_plus_one, rest} = decode_unsigned_varint(data)

    case len_plus_one do
      0 ->
        {nil, rest}

      1 ->
        {[], rest}

      n ->
        len = n - 1

        Enum.reduce(1..len, {[], rest}, fn _ix, {acc, d} ->
          {val, r} = deserialize(type, d)
          {[val | acc], r}
        end)
    end
  end

  def deserialize_compact_array(data, deserializer_fn) when is_function(deserializer_fn, 1) do
    {len_plus_one, rest} = decode_unsigned_varint(data)

    case len_plus_one do
      0 ->
        {nil, rest}

      1 ->
        {[], rest}

      n ->
        len = n - 1

        Enum.reduce(1..len, {[], rest}, fn _ix, {acc, d} ->
          {val, r} = deserializer_fn.(d)
          {[val | acc], r}
        end)
    end
  end

  # Tagged fields (KIP-482)
  def deserialize_tagged_fields(data) do
    {num_tags, rest} = decode_unsigned_varint(data)

    if num_tags == 0 do
      {[], rest}
    else
      Enum.reduce(1..num_tags, {[], rest}, fn _ix, {acc, d} ->
        {tag, d1} = decode_unsigned_varint(d)
        {size, d2} = decode_unsigned_varint(d1)
        <<tag_data::size(size)-binary, d3::binary>> = d2
        {[{tag, tag_data} | acc], d3}
      end)
    end
  end

  def decode_unsigned_varint(data), do: decode_unsigned_varint_loop(data, 0, 0)

  defp decode_unsigned_varint_loop(<<byte, rest::binary>>, acc, shift) do
    value = Bitwise.bor(acc, Bitwise.bsl(Bitwise.band(byte, 0x7F), shift))

    if Bitwise.band(byte, 0x80) == 0 do
      {value, rest}
    else
      decode_unsigned_varint_loop(rest, value, shift + 7)
    end
  end
end
