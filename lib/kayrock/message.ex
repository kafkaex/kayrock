defmodule Kayrock.Message do
  @moduledoc """
  Used for message data, both in requests and responses
  """

  use Bitwise

  defstruct offset: nil, compression: :none, key: nil, value: nil, attributes: nil, crc: nil

  def serialize(messages) when is_list(messages) do
    [%__MODULE__{compression: compression} | _] = messages
    # note when we serialize we never have an offset
    {message, msize} = create_message_set(messages, compression)
    [<<msize::32-signed>>, message]
  end

  def deserialize_message_set(msg_set_size, msg_set_data)
      when byte_size(msg_set_data) == msg_set_size do
    deserialize_messages(msg_set_data, [])
  end

  def deserialize_message_set(msg_set_size, partial_message_data) do
    raise RuntimeError,
          "Insufficient data fetched. Message size is #{msg_set_size} but only " <>
            "received #{byte_size(partial_message_data)} bytes. Try increasing max_bytes."
  end

  defp deserialize_messages(
         <<offset::64-signed, msg_size::32-signed, msg::size(msg_size)-binary, orig_rest::bits>>,
         acc
       ) do
    <<crc::32, 0::8-signed, attributes::8-signed, rest::bits>> = msg

    {key, rest} = deserialize_string(rest)
    {value, <<>>} = deserialize_string(rest)

    msg =
      case compression_from_attributes(attributes) do
        0 ->
          %__MODULE__{
            offset: offset,
            crc: crc,
            attributes: attributes,
            key: key,
            value: value
          }

        c ->
          decompressed = Kayrock.Compression.decompress(c, value)
          Enum.reverse(deserialize_messages(decompressed, []))
      end

    deserialize_messages(orig_rest, [msg | acc])
  end

  defp deserialize_messages(_, acc) do
    Enum.reverse(List.flatten(acc))
  end

  # the 2 lsb specifies compression
  defp compression_from_attributes(a), do: a &&& 3

  defp create_message_set([], _compression_type), do: {"", 0}

  defp create_message_set(messages, :none) do
    create_message_set_uncompressed(messages)
  end

  defp create_message_set(messages, compression_type) do
    alias Kayrock.Compression
    {message_set, _} = create_message_set(messages, :none)

    {compressed_message_set, attribute} = Compression.compress(compression_type, message_set)

    {message, msize} = create_message(compressed_message_set, nil, attribute)

    {[<<0::64-signed>>, <<msize::32-signed>>, message], 8 + 4 + msize}
  end

  defp create_message_set_uncompressed([
         %__MODULE__{key: key, value: value} | messages
       ]) do
    {message, msize} = create_message(value, key)
    message_set = [<<0::64-signed>>, <<msize::32-signed>>, message]
    {message_set2, ms2size} = create_message_set(messages, :none)
    {[message_set, message_set2], 8 + 4 + msize + ms2size}
  end

  defp create_message(value, key, attributes \\ 0) do
    {bkey, skey} = bytes(key)
    {bvalue, svalue} = bytes(value)
    sub = [<<0::8, attributes::8-signed>>, bkey, bvalue]
    crc = :erlang.crc32(sub)
    {[<<crc::32>>, sub], 4 + 2 + skey + svalue}
  end

  defp deserialize_string(<<-1::32-signed, rest::bits>>), do: {nil, rest}

  defp deserialize_string(<<str_size::32-signed, str::size(str_size)-binary, rest::bits>>),
    do: {str, rest}

  defp bytes(nil), do: {<<-1::32-signed>>, 4}

  defp bytes(data) do
    case :erlang.iolist_size(data) do
      0 -> {<<0::32>>, 4}
      size -> {[<<size::32>>, data], 4 + size}
    end
  end
end
