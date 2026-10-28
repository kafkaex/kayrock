defmodule Kayrock.Test.RecordBatchFixture do
  @moduledoc """
  One hand-derived magic-v2 record batch (one record with a key, a value and a
  header), used as the `records` payload in the Produce / Fetch flexible
  version fixtures (`compact_records`).

  The bytes follow the Kafka record batch format
  (https://kafka.apache.org/documentation/#recordbatch); the CRC is
  CRC-32C over everything from `attributes` to the end of the batch, checked
  by `crc_valid?/0`.
  """

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  @timestamp 1_700_000_000_000

  @crc_covered <<
    # attributes (no compression, create time, not transactional)
    0::16,
    # last_offset_delta
    0::32,
    # first_timestamp, max_timestamp
    @timestamp::64,
    @timestamp::64,
    # producer_id, producer_epoch, base_sequence (non-idempotent)
    -1::64-signed,
    -1::16-signed,
    -1::32-signed,
    # record count
    1::32,
    # record: length 12 (zigzag varint 24), attributes 0, timestamp_delta 0,
    # offset_delta 0, key "k" (varint len 1 = 2), value "v",
    # 1 header {key "h", value "1"}
    24,
    0,
    0,
    0,
    2,
    "k",
    2,
    "v",
    2,
    2,
    "h",
    2,
    "1"
  >>

  @crc 0x675A95FD

  @after_length <<-1::32-signed, 2::8, @crc::32, @crc_covered::binary>>

  @batch <<0::64, byte_size(@after_length)::32, @after_length::binary>>

  @doc "The record batch bytes (no outer length prefix), 74 bytes"
  @spec binary :: binary()
  def binary, do: @batch

  @doc "The batch as a struct to put in a Produce request"
  @spec request_batch :: RecordBatch.t()
  def request_batch do
    %RecordBatch{
      records: [
        %Record{
          key: "k",
          value: "v",
          timestamp: @timestamp,
          headers: [%RecordHeader{key: "h", value: "1"}]
        }
      ]
    }
  end

  @doc "What `Kayrock.RecordBatch.deserialize/2` returns for `binary/0`"
  @spec decoded :: [RecordBatch.t()]
  def decoded do
    [
      %RecordBatch{
        batch_offset: 0,
        batch_length: byte_size(@after_length),
        partition_leader_epoch: -1,
        crc: @crc,
        attributes: 0,
        last_offset_delta: 0,
        first_timestamp: @timestamp,
        max_timestamp: @timestamp,
        producer_id: -1,
        producer_epoch: -1,
        base_sequence: -1,
        records: [
          %Record{
            attributes: 0,
            timestamp: @timestamp,
            offset: 0,
            key: "k",
            value: "v",
            headers: [%RecordHeader{key: "h", value: "1"}]
          }
        ]
      }
    ]
  end

  @doc "Whether the fixture's CRC is the CRC-32C of the covered bytes"
  @spec crc_valid? :: boolean()
  def crc_valid?, do: :crc32cer.nif(@crc_covered) == @crc
end
