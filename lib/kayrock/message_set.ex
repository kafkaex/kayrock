defmodule Kayrock.MessageSet do
  @moduledoc """
  Represents a set of messages with the v0 or v1 format

  This is the old format that KafkaEx supported

  See https://kafka.apache.org/documentation/#recordbatch
  """

  defmodule Message do
    @moduledoc """
    Represents a single message with the v0 or v1 format

    This is the old format that KafkaEx supported

    See https://kafka.apache.org/documentation/#recordbatch
    """
    defstruct offset: nil,
              compression: :none,
              key: nil,
              value: nil,
              attributes: nil,
              crc: nil,
              timestamp: nil,
              timestamp_type: nil
  end

  use Bitwise

  defstruct messages: [], magic: 0

  def serialize(%__MODULE__{messages: messages}) when is_list(messages) do
    [%Message{compression: compression} | _] = messages
    # note when we serialize we never have an offset
    {message, msize} = create_message_set(messages, compression)
    [<<msize::32-signed>>, message]
  end

  def deserialize(data), do: deserialize(data, 0)

  def deserialize(data, magic) do
    msgs = do_deserialize(data, [], 0)
    %__MODULE__{messages: msgs, magic: magic}
  end

  defp do_deserialize(
         <<offset::64-signed, msg_size::32-signed, msg::size(msg_size)-binary, orig_rest::bits>>,
         acc,
         add_offset
       ) do
    <<crc::32, magic::8-signed, attributes::8-signed, rest::bits>> = msg

    {timestamp, rest} =
      case magic do
        0 -> {nil, rest}
        1 -> Kayrock.Deserialize.deserialize(:int64, rest)
      end

    {key, rest} = deserialize_string(rest)
    {value, <<>>} = deserialize_string(rest)

    msg =
      case compression_from_attributes(attributes) do
        0 ->
          timestamp_type = timestamp_type_from_attributes(attributes, magic)

          %Message{
            offset: offset + add_offset,
            crc: crc,
            attributes: attributes,
            key: key,
            value: value,
            timestamp: timestamp,
            timestamp_type: timestamp_type
          }

        c ->
          decompressed = Kayrock.Compression.decompress(c, value)

          if magic == 1 do
            Enum.reverse(do_deserialize(decompressed, [], offset))
          else
            Enum.reverse(do_deserialize(decompressed, [], 0))
          end
      end

    do_deserialize(orig_rest, [msg | acc], add_offset)
  end

  defp do_deserialize(_, acc, _add_offset) do
    Enum.reverse(List.flatten(acc))
  end

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
         %Message{key: key, value: value} | messages
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

  # the 3 lsb specifies compression
  defp compression_from_attributes(a), do: a &&& 7

  defp timestamp_type_from_attributes(a, 1), do: a &&& 8
  defp timestamp_type_from_attributes(_, _), do: nil

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
