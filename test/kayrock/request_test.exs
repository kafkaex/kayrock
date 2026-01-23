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
end
