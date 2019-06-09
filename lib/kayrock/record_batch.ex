defmodule Kayrock.RecordBatch do
  defmodule Record do
    defstruct(
      attributes: nil,
      timestamp: -1,
      offset: nil,
      key: nil,
      value: nil,
      headers: <<0>>
    )
  end

  defstruct(
    batch_offset: nil,
    batch_length: nil,
    partition_leader_epoch: nil,
    crc: nil,
    attributes: nil,
    last_offset_delta: nil,
    first_timestamp: nil,
    max_timestamp: nil,
    producer_id: nil,
    producer_epoch: nil,
    base_sequence: nil,
    records: []
  )

  alias Kayrock.MessageSet

  def serialize(%__MODULE__{}) do
    # TODO
    <<>>
  end

  @doc """
  Direct deserializer

  Supplied for compatibility with the Request protocol
  """
  def deserialize(record_batch_data) do
    deserialize(byte_size(record_batch_data), record_batch_data)
  end

  def deserialize(msg_set_size, msg_set_data)
      when byte_size(msg_set_data) == msg_set_size do
    case get_magic_byte(msg_set_data) do
      {2, batch_offset, batch_length, partition_leader_epoch, rest} ->
        deserialize(batch_offset, batch_length, partition_leader_epoch, rest)

      {magic, _, _, _, _} ->
        # old code
        MessageSet.deserialize(msg_set_data, magic)
    end
  end

  def deserialize(msg_set_size, partial_message_data) do
    raise RuntimeError,
          "Insufficient data fetched. Message size is #{msg_set_size} but only " <>
            "received #{byte_size(partial_message_data)} bytes. Try increasing max_bytes."
  end

  defp deserialize(batch_offset, batch_length, partition_leader_epoch, rest) do
    <<crc::32-signed, attributes::16-signed, last_offset_delta::32-signed,
      first_timestamp::64-signed, max_timestamp::64-signed, producer_id::64-signed,
      producer_epoch::16-signed, base_sequence::32-signed, num_msgs::32-signed,
      rest::bits>> = rest

    msgs = deserialize_message(rest, num_msgs, [])

    msgs =
      Enum.map(msgs, fn msg ->
        %{msg | offset: msg.offset + batch_offset, timestamp: msg.timestamp + first_timestamp}
      end)

    %__MODULE__{
      batch_offset: batch_offset,
      batch_length: batch_length,
      partition_leader_epoch: partition_leader_epoch,
      crc: crc,
      attributes: attributes,
      last_offset_delta: last_offset_delta,
      first_timestamp: first_timestamp,
      max_timestamp: max_timestamp,
      producer_id: producer_id,
      producer_epoch: producer_epoch,
      base_sequence: base_sequence,
      records: msgs
    }
  end

  defp deserialize_message(_data, 0, acc) do
    Enum.reverse(acc)
  end

  defp deserialize_message(data, num_left, acc) do
    {msg_size, rest} = decode_varint(data)
    msg_size = msg_size - 1
    <<attributes::8-signed, msg_data::size(msg_size)-binary, orest::bits>> = rest

    {timestamp_delta, rest} = decode_varint(msg_data)
    {offset_delta, rest} = decode_varint(rest)
    {key_length, rest} = decode_varint(rest)

    {key, rest} =
      case key_length do
        -1 ->
          {nil, rest}

        len ->
          <<key::size(len)-binary, rest::bits>> = rest
          {key, rest}
      end

    {value_length, rest} = decode_varint(rest)

    {value, rest} =
      case value_length do
        -1 ->
          {nil, rest}

        len ->
          <<value::size(len)-binary, rest::bits>> = rest
          {value, rest}
      end

    msg = %Record{
      attributes: attributes,
      key: key,
      value: value,
      offset: offset_delta,
      timestamp: timestamp_delta,
      headers: rest
    }

    deserialize_message(orest, num_left - 1, [msg | acc])
  end

  defp decode_varint(data) do
    {val, rest} = Varint.LEB128.decode(data)
    {Varint.Zigzag.decode(val), rest}
  end

  # newest version (called v2 here):
  # baseOffset: int64
  # batchLength: int32
  # partitionLeaderEpoch: int32
  # magic: int8 (current magic value is 2)
  #
  # v0 and v1:
  # offset: int64
  # message_size: int32
  # first_message crc: int32
  # first_message magic: int8
  defp get_magic_byte(msg_set_data) do
    <<first_offset::64-signed, batch_length_or_message_size::32-signed,
      partition_leader_epoch_or_first_crc::32-signed, magic::8-signed, rest::bits>> = msg_set_data

    {magic, first_offset, batch_length_or_message_size, partition_leader_epoch_or_first_crc, rest}
  end
end
