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

  def deserialize(:boolean, <<val, rest::binary>>), do: {val, rest}
  def deserialize(:int8, <<val::8-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int16, <<val::16-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int32, <<val::32-signed, rest::binary>>), do: {val, rest}
  def deserialize(:int64, <<val::64-signed, rest::binary>>), do: {val, rest}

  def deserialize(:string, <<len::16-signed, val::size(len)-binary, rest::binary>>) do
    {val, rest}
  end

  def deserialize(:nullable_string, <<-1::16-signed, rest::binary>>), do: {nil, rest}

  def deserialize(:nullable_string, data), do: deserialize(:string, data)

  def deserialize(:bytes, <<len::32-signed, val::size(len)-binary, rest::binary>>) do
    {val, rest}
  end

  def deserialize(:nullable_bytes, <<-1::32-signed, rest::binary>>), do: {nil, rest}
  def deserialize(:nullable_bytes, data), do: deserialize(:bytes, data)

  def deserialize_array(_type, <<0::32-signed, rest::bits>>), do: {[], rest}

  def deserialize_array(type, <<len::32-signed, rest::bits>>) do
    Enum.reduce(1..len, {[], rest}, fn _ix, {acc, d} ->
      {val, r} = deserialize(type, d)
      {[val | acc], r}
    end)
  end
end
