defmodule Kayrock.RecordBatch do
  @moduledoc """
  Represents a batch of records (also called a record set)

  This is the newer format in Kafka 0.11+

  See https://kafka.apache.org/documentation/#recordbatch
  """

  defmodule Record do
    @moduledoc """
    Represents a single record (message)

    This is the newer format in Kafka 0.11+

    See https://kafka.apache.org/documentation/#recordbatch
    """
    defstruct(
      attributes: 0,
      timestamp: -1,
      offset: 0,
      key: nil,
      value: nil,
      headers: []
    )

    @type t :: %__MODULE__{}
  end

  defmodule RecordHeader do
    @moduledoc """
    Represents a record header

    Headers were added on Kafka 0.11+

    See https://kafka.apache.org/documentation/#recordheader
    """
    defstruct(
      key: nil,
      value: nil
    )

    @type t :: %__MODULE__{}
  end

  defstruct(
    batch_offset: 0,
    batch_length: nil,
    partition_leader_epoch: -1,
    crc: nil,
    attributes: 0,
    last_offset_delta: -1,
    first_timestamp: -1,
    max_timestamp: -1,
    producer_id: -1,
    producer_epoch: -1,
    base_sequence: -1,
    records: []
  )

  @type t :: %__MODULE__{}

  import Bitwise

  alias Kayrock.Compression
  alias Kayrock.MessageSet

  @spec from_binary_list([binary], atom) :: t
  def from_binary_list(messages, compression \\ :none) when is_list(messages) do
    %__MODULE__{
      attributes: set_compression_bits(0, compression),
      records: Enum.map(messages, fn m -> %Record{value: m} end)
    }
  end

  # baseOffset: int64
  # batchLength: int32
  # partitionLeaderEpoch: int32
  # magic: int8 (current magic value is 2)
  # crc: int32
  # attributes: int16
  #     bit 0~2:
  #         0: no compression
  #         1: gzip
  #         2: snappy
  #         3: lz4
  #         4: zstd
  #     bit 3: timestampType
  #     bit 4: isTransactional (0 means not transactional)
  #     bit 5: isControlBatch (0 means not a control batch)
  #     bit 6~15: unused
  # lastOffsetDelta: int32
  # firstTimestamp: int64
  # maxTimestamp: int64
  # producerId: int64
  # producerEpoch: int16
  # baseSequence: int32
  # records: [Record]

  @spec serialize(t) :: iodata
  def serialize(%__MODULE__{} = record_batch) do
    [first_record | _] = record_batch.records
    num_records = length(record_batch.records)

    max_timestamp =
      record_batch.records
      |> Enum.map(fn r -> r.timestamp end)
      |> Enum.max()

    base_timestamp = first_record.timestamp

    records =
      record_batch.records
      |> Enum.with_index()
      |> Enum.map(fn {record, offset_delta} ->
        record
        |> normalize_record(offset_delta, base_timestamp)
        |> serialize_record()
      end)

    records =
      case compression_type(record_batch.attributes) do
        :none ->
          records

        other ->
          {compressed_records, _} = Compression.compress(other, records)
          compressed_records
      end

    last_offset_delta = num_records - 1

    header =
      <<record_batch.attributes::16-signed, last_offset_delta::32-signed,
        first_record.timestamp::64-signed, max_timestamp::64-signed,
        record_batch.producer_id::64-signed, record_batch.producer_epoch::16-signed,
        record_batch.base_sequence::32-signed, num_records::32-signed>>

    for_crc = [header, records]
    crc = :crc32cer.nif(for_crc)

    for_size = [
      <<record_batch.partition_leader_epoch::32-signed, 2::8-signed, crc::32-signed>>,
      for_crc
    ]

    v = [<<first_record.offset::64-signed, IO.iodata_length(for_size)::32-signed>>, for_size]
    [<<IO.iodata_length(v)::32-signed>>, v]
  end

  @doc """
  Direct deserializer
  Supplied for compatibility with the Request protocol
  """
  @spec deserialize(binary) :: nil | MessageSet.t() | t
  def deserialize(record_batch_data) do
    deserialize(byte_size(record_batch_data), record_batch_data)
  end

  @spec deserialize(integer, binary) :: nil | MessageSet.t() | t
  def deserialize(0, "") do
    nil
  end

  def deserialize(msg_set_size, msg_set_data) when byte_size(msg_set_data) == msg_set_size do
    case get_magic_byte(msg_set_data) do
      {2, batch_offset, batch_length, partition_leader_epoch, rest} ->
        deserialize(rest, [], batch_offset, batch_length, partition_leader_epoch)

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

  defp deserialize(rest, acc, batch_offset, batch_length, partition_leader_epoch) do
    # we already parsed off 5 bytes in get_magic_byte
    real_size = batch_length - 5

    deserialize(real_size, rest, acc, batch_offset, batch_length, partition_leader_epoch)
  end

  # If the expected size to fetch is less than the rest of the body, we have
  # fetched an incomplete record. Cowardly refuse to parse this record.
  defp deserialize(real_size, rest, acc, _, _, _) when real_size > byte_size(rest) do
    Enum.reverse(acc)
  end

  defp deserialize(real_size, rest, acc, batch_offset, batch_length, partition_leader_epoch) do
    <<batch_data::size(real_size)-binary, body_rest::binary>> = rest

    <<crc::32-signed, attributes::16-signed, last_offset_delta::32-signed,
      first_timestamp::64-signed, max_timestamp::64-signed, producer_id::64-signed,
      producer_epoch::16-signed, base_sequence::32-signed, num_msgs::32-signed,
      batch_rest::bits>> = batch_data

    msg_data =
      case compression_from_attributes(attributes) do
        0 ->
          batch_rest

        other ->
          Compression.decompress(other, batch_rest)
      end

    {msgs, _trailing_bytes} = deserialize_message(msg_data, num_msgs, [])

    # NOTE we sometimes get record batches with all offset_delta = 0
    # we use the order in the record batch to determine offset in that case
    #
    # NOTE we sometimes get first_timestamp = -1 and max_timestamp = <actual
    # timestamp>, so we also normalize for that
    msgs =
      msgs
      |> Enum.zip(0..(num_msgs - 1))
      |> Enum.map(fn {msg, ix} ->
        %{
          msg
          | offset: determine_offset(batch_offset, msg.offset, ix),
            timestamp: determine_timestamp(first_timestamp, max_timestamp, msg.timestamp)
        }
      end)

    record_batch = %__MODULE__{
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

    acc = [record_batch | acc]

    case get_magic_byte(body_rest) do
      {2, batch_offset, batch_length, partition_leader_epoch, new_rest} ->
        deserialize(new_rest, acc, batch_offset, batch_length, partition_leader_epoch)

      _ ->
        Enum.reverse(acc)
    end
  end

  defp deserialize_message(data, 0, acc) do
    {Enum.reverse(acc), data}
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

    {headers_length, rest} = decode_varint(rest)
    headers = deserialize_msg_headers(rest, headers_length, [])

    msg = %Record{
      attributes: attributes,
      key: key,
      value: value,
      offset: offset_delta,
      timestamp: timestamp_delta,
      headers: headers
    }

    deserialize_message(orest, num_left - 1, [msg | acc])
  end

  defp deserialize_msg_headers(_data, 0, acc) do
    Enum.reverse(acc)
  end

  defp deserialize_msg_headers(data, headers_left, acc) do
    {key_len, rest} = decode_varint(data)

    <<key::size(key_len)-binary, rest::bits>> = rest

    {value_len, rest} = decode_varint(rest)

    {value, rest} =
      case value_len do
        -1 ->
          {nil, rest}

        len ->
          <<val::size(len)-binary, rest::bits>> = rest
          {val, rest}
      end

    header = %Kayrock.RecordBatch.RecordHeader{key: key, value: value}

    deserialize_msg_headers(rest, headers_left - 1, [header | acc])
  end

  defp decode_varint(data) do
    {val, rest} = Varint.LEB128.decode(data)
    {Varint.Zigzag.decode(val), rest}
  end

  defp encode_varint(v) do
    v
    |> Varint.Zigzag.encode()
    |> Varint.LEB128.encode()
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
  # Return early if we do not have a complete 17 bytes to parse from the record
  defp get_magic_byte(msg_set_data) when byte_size(msg_set_data) < 17, do: nil

  defp get_magic_byte(msg_set_data) do
    <<first_offset::64-signed, batch_length_or_message_size::32-signed,
      partition_leader_epoch_or_first_crc::32-signed, magic::8-signed, rest::bits>> = msg_set_data

    {magic, first_offset, batch_length_or_message_size, partition_leader_epoch_or_first_crc, rest}
  end

  # length: varint
  # attributes: int8
  #     bit 0~7: unused
  # timestampDelta: varint
  # offsetDelta: varint
  # keyLength: varint
  # key: byte[]
  # valueLen: varint
  # value: byte[]
  # Headers => [Header]
  defp serialize_record(record) do
    key_size =
      case record.key do
        nil -> -1
        bin -> byte_size(bin)
      end

    value_size =
      case record.value do
        nil -> -1
        bin -> byte_size(bin)
      end

    without_length =
      Enum.filter(
        [
          <<record.attributes::8-signed>>,
          encode_varint(record.timestamp),
          encode_varint(record.offset),
          encode_varint(key_size),
          record.key,
          encode_varint(value_size),
          record.value,
          serialize_record_headers(record.headers)
        ],
        fn x -> !is_nil(x) end
      )

    [encode_varint(IO.iodata_length(without_length)), without_length]
  end

  defp normalize_record(record, offset_delta, base_timestamp) do
    %{
      record
      | timestamp: maybe_delta(record.timestamp, base_timestamp),
        offset: offset_delta
    }
  end

  defp serialize_record_headers(headers) when is_nil(headers) do
    <<0>>
  end

  defp serialize_record_headers([] = _headers) do
    <<0>>
  end

  defp serialize_record_headers(headers) do
    headers_len = encode_varint(length(headers))

    Enum.reduce(headers, headers_len, fn header, acc ->
      acc <> serialize_record_header(header)
    end)
  end

  defp serialize_record_header(%RecordHeader{key: key, value: value})
       when not is_nil(key) do
    encoded_value =
      if is_nil(value) do
        encode_varint(-1)
      else
        encode_varint(byte_size(value)) <> <<value::binary>>
      end

    encode_varint(byte_size(key)) <> <<key::binary>> <> encoded_value
  end

  defp maybe_delta(nil, _), do: nil
  defp maybe_delta(x, y), do: x - y

  defp compression_type(attributes) do
    case attributes &&& 7 do
      0 -> :none
      1 -> :gzip
      2 -> :snappy
      3 -> :lz4
      4 -> :zstd
    end
  end

  defp set_compression_bits(val, :none), do: val
  defp set_compression_bits(val, :gzip), do: set_compression_bits(val, 1)
  defp set_compression_bits(val, :snappy), do: set_compression_bits(val, 2)
  defp set_compression_bits(val, :lz4), do: set_compression_bits(val, 3)
  defp set_compression_bits(val, :zstd), do: set_compression_bits(val, 4)

  defp set_compression_bits(val, compression_val)
       when is_integer(compression_val) and compression_val > 0 and compression_val <= 4 do
    (val &&& bnot(7)) + compression_val
  end

  defp determine_offset(batch_offset, 0, ix), do: batch_offset + ix
  defp determine_offset(batch_offset, offset_delta, _ix), do: batch_offset + offset_delta

  defp determine_timestamp(-1, max_timestamp, timestamp_offset)
       when is_integer(max_timestamp) and max_timestamp > 0 do
    max_timestamp + timestamp_offset
  end

  defp determine_timestamp(first_timestamp, _, timestamp_offset) do
    first_timestamp + timestamp_offset
  end

  # the 2 lsb specifies compression
  defp compression_from_attributes(a), do: a &&& 7
end
