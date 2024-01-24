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

    @type t :: %__MODULE__{}
  end

  import Bitwise

  defstruct messages: [], magic: 0

  @type t :: %__MODULE__{}

  @spec serialize(t) :: iodata
  def serialize(%__MODULE__{messages: messages}) when is_list(messages) do
    [%Message{compression: compression} | _] = messages
    # note when we serialize we never have an offset
    {message, msize} = create_message_set(messages, compression)
    [<<msize::32-signed>>, message]
  end

  @spec deserialize(binary) :: t
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
          c
          |> Kayrock.Compression.decompress(value)
          |> do_deserialize([], 0)
          |> correct_offsets(offset)
          |> Enum.reverse()
      end

    do_deserialize(orig_rest, [msg | acc], add_offset)
  end

  defp do_deserialize(_, acc, _add_offset) do
    Enum.reverse(List.flatten(acc))
  end

  # All other cases, compressed inner messages should have relative offset, with below attributes:
  #   - The container message should have the 'real' offset
  #   - The container message's offset should be the 'real' offset of the last message in the compressed batch
  defp correct_offsets(messages, real_offset) do
    max_relative_offset = messages |> List.last() |> Map.fetch!(:offset)

    if max_relative_offset == real_offset do
      messages
    else
      Enum.map(messages, fn msg ->
        Map.update!(msg, :offset, &(&1 + real_offset))
      end)
    end
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
