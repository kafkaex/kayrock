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

  def serialize_array(_type, nil), do: <<-1::32-signed>>
  def serialize_array(_type, []), do: <<0::32-signed>>

  def serialize_array(type, vals) when is_list(vals) do
    [
      <<length(vals)::32-signed>>,
      Enum.map(vals, &serialize(type, &1))
    ]
  end
end
