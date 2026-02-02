defmodule Kayrock.RequestTest do
  @moduledoc """
  Tests for Kayrock.Request protocol and implementations.
  """
  use ExUnit.Case, async: true

  alias Kayrock.InvalidRequestError
  alias Kayrock.MessageSet
  alias Kayrock.MessageSet.Message
  alias Kayrock.RecordBatch
  alias Kayrock.Request

  describe "InvalidRequestError" do
    test "creates exception with message" do
      error = %ArgumentError{message: "bad arg"}
      request = %{some: "struct"}

      exception = InvalidRequestError.exception({error, request})
      assert exception.message =~ "Error serializing request struct"
      assert exception.message =~ "bad arg"
      assert exception.message =~ "some"
    end
  end

  describe "Request protocol for RecordBatch" do
    test "api_vsn returns 2" do
      batch = %RecordBatch{records: []}
      assert Request.api_vsn(batch) == 2
    end

    test "serialize delegates to RecordBatch.serialize" do
      batch = %RecordBatch{
        records: [
          %RecordBatch.Record{
            key: "k",
            value: "v",
            headers: [],
            attributes: 0,
            timestamp: -1,
            offset: 0
          }
        ]
      }

      serialized = Request.serialize(batch)
      assert is_list(serialized) or is_binary(serialized)
    end

    test "response_deserializer returns a function" do
      batch = %RecordBatch{records: []}
      deserializer = Request.response_deserializer(batch)
      assert is_function(deserializer, 1)
    end
  end

  describe "Request protocol for MessageSet" do
    test "api_vsn returns 1" do
      message_set = %MessageSet{messages: []}
      assert Request.api_vsn(message_set) == 1
    end

    test "serialize delegates to MessageSet.serialize" do
      message_set = %MessageSet{
        messages: [%Message{key: "k", value: "v"}]
      }

      serialized = Request.serialize(message_set)
      assert is_list(serialized) or is_binary(serialized)
    end

    test "response_deserializer returns a function" do
      message_set = %MessageSet{messages: []}
      deserializer = Request.response_deserializer(message_set)
      assert is_function(deserializer, 1)
    end
  end

  # ============================================
  # Binary Integrity Tests
  # ============================================

  describe "serialization binary integrity" do
    test "produces deterministic output" do
      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "test-client",
        topics: [%{name: "my-topic"}]
      }

      serialized1 = IO.iodata_to_binary(Request.serialize(request))
      serialized2 = IO.iodata_to_binary(Request.serialize(request))

      assert serialized1 == serialized2
    end

    test "includes correct API key and version in header" do
      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 123,
        client_id: "test",
        topics: []
      }

      serialized = IO.iodata_to_binary(Request.serialize(request))

      <<api_key::16, api_version::16, correlation_id::32, _rest::binary>> = serialized

      assert api_key == 3, "Metadata API key should be 3"
      assert api_version == 0, "Version should be 0"
      assert correlation_id == 123, "Correlation ID should match"
    end

    test "encodes strings with correct length prefix" do
      request = %Kayrock.Metadata.V0.Request{
        correlation_id: 1,
        client_id: "hello",
        topics: []
      }

      serialized = IO.iodata_to_binary(Request.serialize(request))

      # Skip header: api_key(2) + api_version(2) + correlation_id(4) = 8 bytes
      <<_header::binary-size(8), client_id_len::16, client_id::binary-size(client_id_len),
        _rest::binary>> = serialized

      assert client_id_len == 5
      assert client_id == "hello"
    end
  end

  # ============================================
  # Response Deserialization Edge Cases
  # ============================================

  describe "response deserialization edge cases" do
    test "response with different correlation_id still deserializes" do
      # Kayrock doesn't validate correlation_id during deserialization
      # (that's the client's responsibility)
      response_binary =
        Kayrock.Test.Factories.MetadataFactory.response_binary(0,
          correlation_id: 999,
          brokers: [],
          topics: []
        )

      {response, <<>>} = Kayrock.Metadata.V0.Response.deserialize(response_binary)

      assert response.correlation_id == 999
    end

    test "response deserializer preserves trailing bytes" do
      response_binary =
        Kayrock.Test.Factories.MetadataFactory.response_binary(0,
          correlation_id: 1,
          brokers: [],
          topics: []
        )

      extra_bytes = <<1, 2, 3, 4, 5>>
      combined = response_binary <> extra_bytes

      {_response, rest} = Kayrock.Metadata.V0.Response.deserialize(combined)

      assert rest == extra_bytes, "Trailing bytes should be preserved"
    end
  end
end
