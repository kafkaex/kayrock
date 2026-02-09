defmodule Kayrock.RecordBatchTest do
  @moduledoc """
  Tests for Kayrock.RecordBatch module - record batch serialization/deserialization.
  """
  use ExUnit.Case, async: true

  alias Kayrock.RecordBatch
  alias Kayrock.RecordBatch.Record
  alias Kayrock.RecordBatch.RecordHeader

  describe "serialize/deserialize round-trip" do
    test "preserves data with keys and values" do
      original = %RecordBatch{
        attributes: 0,
        records: [
          %Record{value: "message-1", key: "key-1", headers: []},
          %Record{value: "message-2", key: nil, headers: []},
          %Record{value: <<0, 1, 2, 255>>, key: "binary-key", headers: []}
        ]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(original))

      # Strip 4-byte length prefix for deserialization
      <<_length::32, batch_data::binary>> = serialized
      batch_size = byte_size(batch_data)

      [deserialized] = RecordBatch.deserialize(batch_size, batch_data)

      assert length(deserialized.records) == 3
      assert Enum.at(deserialized.records, 0).value == "message-1"
      assert Enum.at(deserialized.records, 0).key == "key-1"
      assert Enum.at(deserialized.records, 1).value == "message-2"
      assert Enum.at(deserialized.records, 1).key == nil
      assert Enum.at(deserialized.records, 2).value == <<0, 1, 2, 255>>
    end

    test "preserves headers" do
      original = %RecordBatch{
        attributes: 0,
        records: [
          %Record{
            value: "test",
            key: nil,
            headers: [
              %RecordHeader{key: "header-key", value: "header-value"},
              %RecordHeader{key: "trace-id", value: "abc123"}
            ]
          }
        ]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(original))

      <<_length::32, batch_data::binary>> = serialized
      batch_size = byte_size(batch_data)

      [deserialized] = RecordBatch.deserialize(batch_size, batch_data)

      [record] = deserialized.records
      assert length(record.headers) == 2

      [h1, h2] = record.headers
      assert h1.key == "header-key"
      assert h1.value == "header-value"
      assert h2.key == "trace-id"
      assert h2.value == "abc123"
    end

    test "gzip compression round-trips" do
      original = %RecordBatch{
        # gzip compression
        attributes: 1,
        records: [
          %Record{value: String.duplicate("compress me! ", 100), key: nil, headers: []}
        ]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(original))

      <<_length::32, batch_data::binary>> = serialized
      batch_size = byte_size(batch_data)

      [deserialized] = RecordBatch.deserialize(batch_size, batch_data)

      assert deserialized.attributes == 1
      assert length(deserialized.records) == 1
      [record] = deserialized.records
      assert record.value == String.duplicate("compress me! ", 100)
    end
  end

  describe "deserialize edge cases" do
    test "empty binary returns nil" do
      assert RecordBatch.deserialize(<<>>) == nil
    end

    test "mismatched size raises RuntimeError" do
      batch = %RecordBatch{
        attributes: 0,
        records: [%Record{value: "test", key: nil, headers: []}]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(batch))
      <<_length::32, batch_data::binary>> = serialized

      # Claim a larger size than actual data
      claimed_size = byte_size(batch_data) + 100

      assert_raise RuntimeError, ~r/Insufficient data fetched/, fn ->
        RecordBatch.deserialize(claimed_size, batch_data)
      end
    end

    test "truncated mid-record raises RuntimeError" do
      batch = %RecordBatch{
        attributes: 0,
        records: [%Record{value: String.duplicate("x", 100), key: nil, headers: []}]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(batch))

      <<_length::32, batch_data::binary>> = serialized
      full_size = byte_size(batch_data)

      # Truncate in the middle of the record data
      truncated = binary_part(batch_data, 0, div(full_size, 2))

      assert_raise RuntimeError, ~r/Insufficient data fetched/, fn ->
        RecordBatch.deserialize(full_size, truncated)
      end
    end

    test "truncated mid-header raises error" do
      batch = %RecordBatch{
        attributes: 0,
        records: [%Record{value: "test", key: nil, headers: []}]
      }

      serialized = IO.iodata_to_binary(RecordBatch.serialize(batch))

      # Truncate at 10 bytes (mid batch structure)
      truncated = binary_part(serialized, 0, 10)

      # Should raise some error for insufficient/malformed data
      assert_raise CaseClauseError, fn ->
        RecordBatch.deserialize(truncated)
      end
    end

    test "too few bytes for header detection returns nil or raises" do
      # Less than 17 bytes (minimum for magic byte detection)
      too_small = <<0, 0, 0, 0, 0, 0, 0, 0, 0, 0>>

      result =
        try do
          RecordBatch.deserialize(too_small)
        rescue
          CaseClauseError -> nil
          MatchError -> nil
        end

      assert result == nil or result == []
    end

    test "magic byte 0 falls back to MessageSet" do
      # Magic byte 0 indicates old MessageSet format
      value = "test"
      key_length = -1
      value_length = byte_size(value)

      message_content = <<
        # magic = 0
        0::8,
        # attributes = 0
        0::8,
        # key length (-1 = null)
        key_length::32-signed,
        # value length
        value_length::32,
        # value
        value::binary
      >>

      crc = :erlang.crc32(message_content)
      message_size = byte_size(message_content) + 4

      old_format = <<
        # offset
        0::64,
        # message_size
        message_size::32,
        # crc
        crc::32,
        # message content
        message_content::binary
      >>

      result = RecordBatch.deserialize(old_format)
      assert %Kayrock.MessageSet{} = result
      assert length(result.messages) == 1
      [msg] = result.messages
      assert msg.value == "test"
      assert msg.key == nil
    end
  end
end
