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
        :uint16,
        :float64,
        :uuid,
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

  @doc """
  Fixed-width primitive types (no length prefix), valid as array elements in
  both standard and compact (flexible) arrays
  """
  defmacro fixed_width_types do
    quote do
      [:boolean, :int8, :int16, :int32, :int64, :uint16, :float64, :uuid]
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
        :uint16,
        :float64,
        :uuid,
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

  def serialize(:uint16, val) when is_integer(val) and val >= 0 and val <= 0xFFFF,
    do: <<val::16-unsigned>>

  def serialize(:uint16, val) do
    raise ArgumentError, "uint16 value must be an integer in 0..65535, got: #{inspect(val)}"
  end

  # IEEE 754 binary64, big-endian.  Integers are accepted and widened.
  def serialize(:float64, val) when is_float(val), do: <<val::64-float>>
  def serialize(:float64, val) when is_integer(val), do: <<val * 1.0::64-float>>

  # Kafka's NaN / Infinity cannot be represented by BEAM floats; pass them as
  # their raw 8-byte IEEE 754 encoding instead.
  def serialize(:float64, <<_::64>> = raw), do: raw

  # UUIDs are 16 raw bytes.  `nil` is Kafka's zero (null) UUID.
  def serialize(:uuid, nil), do: <<0::128>>
  def serialize(:uuid, <<_::128>> = val), do: val

  def serialize(:uuid, val) do
    raise ArgumentError,
          "uuid value must be a 16-byte binary (or nil for the zero UUID), got: #{inspect(val)}"
  end

  def serialize(:string, val), do: [<<byte_size(val)::16-signed>>, val]
  def serialize(:nullable_string, nil), do: <<-1::16-signed>>
  def serialize(:nullable_string, val), do: [<<byte_size(val)::16-signed>>, val]
  def serialize(:bytes, val), do: [<<byte_size(val)::32-signed>>, val]
  def serialize(:iodata_bytes, val), do: [<<IO.iodata_length(val)::32-signed>>, val]
  def serialize(:nullable_bytes, nil), do: <<-1::32-signed>>
  def serialize(:nullable_bytes, val), do: serialize(:bytes, val)

  # Compact types (KIP-482) - use unsigned varint, length+1 encoding
  def serialize(:compact_string, nil) do
    raise ArgumentError,
          "compact_string field cannot be nil (non-nullable per Kafka protocol). " <>
            "Set the field to a string value, or use compact_nullable_string for nullable fields."
  end

  def serialize(:compact_string, val) when is_binary(val) do
    [encode_unsigned_varint(byte_size(val) + 1), val]
  end

  def serialize(:compact_nullable_string, nil), do: encode_unsigned_varint(0)

  def serialize(:compact_nullable_string, val) when is_binary(val) do
    serialize(:compact_string, val)
  end

  def serialize(:compact_bytes, nil) do
    raise ArgumentError,
          "compact_bytes field cannot be nil (non-nullable per Kafka protocol). " <>
            "Set the field to a binary value, or use compact_nullable_bytes for nullable fields."
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

  @doc false
  # Compact (flexible-version) record sets: an unsigned-varint length+1 prefix
  # (0 = null) followed by the record set bytes.  The record set (a
  # `Kayrock.RecordBatch` or `Kayrock.MessageSet`) is encoded with
  # `Kayrock.Request.serialize/1`, which emits an int32 length prefix that is
  # replaced here.
  def serialize_compact_records(nil), do: encode_unsigned_varint(0)

  def serialize_compact_records(records) do
    <<size::32-signed, data::binary>> = IO.iodata_to_binary(Kayrock.Request.serialize(records))

    if size == -1 do
      encode_unsigned_varint(0)
    else
      [encode_unsigned_varint(size + 1), data]
    end
  end

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

  @doc false
  # Tagged-field section for a struct with named tagged fields (generated
  # code).  `named` holds `{tag, binary}` for every named tagged field that
  # differs from its default (and `nil` for the others); `raw` is the struct's
  # `tagged_fields` bucket of unknown tags.  Fields are written in ascending
  # tag order, as KIP-482 requires.
  def serialize_tagged_fields(named, raw) when is_list(named) do
    named = Enum.reject(named, &is_nil/1)
    raw = raw || []

    case Enum.filter(raw, fn {tag, _} -> List.keymember?(named, tag, 0) end) do
      [] ->
        serialize_tagged_fields(Enum.sort_by(named ++ raw, &elem(&1, 0)))

      clashes ->
        raise ArgumentError,
              "tagged_fields contains tags #{inspect(Enum.map(clashes, &elem(&1, 0)))} " <>
                "that are also named fields of this struct; set the named field instead"
    end
  end

  @doc false
  # Whether a named tagged field's value equals its default (and so is left
  # off the wire).  `nil` always means "not set" except for nullable types,
  # where it is a value like any other.
  def tagged_field_default?(:nullable, value, default), do: value == default
  def tagged_field_default?(_kind, nil, _default), do: true

  def tagged_field_default?(:boolean, value, default) when is_boolean(value),
    do: tagged_field_value(:boolean, default) == value

  def tagged_field_default?(_kind, value, default), do: value == default

  @doc false
  # Booleans decode as 1 / 0, so a decoded tagged bool is accepted back as
  # its integer form (`serialize(:boolean, 1)` would otherwise write false).
  def tagged_field_value(:boolean, 1), do: true
  def tagged_field_value(:boolean, 0), do: false
  def tagged_field_value(_kind, value), do: value

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
