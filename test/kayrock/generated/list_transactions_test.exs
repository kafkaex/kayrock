defmodule Kayrock.ListTransactionsTest do
  @moduledoc """
  Wire fixtures for ListTransactions (API key 66, v0-v2), derived by hand
  from `priv/kafka_messages/4.4.0/ListTransactions{Request,Response}.json`.

  v1 adds DurationFilter (KIP-994); v2 adds the nullable
  TransactionalIdPattern (KIP-1152). The response layout is the same in
  every version. All versions are flexible.
  """
  use ExUnit.Case, async: true

  @api_key 66

  test "module metadata" do
    assert Kayrock.ListTransactions.min_vsn() == 0
    assert Kayrock.ListTransactions.max_vsn() == 2
    assert Kayrock.KafkaSchemaMetadata.api_key(:list_transactions) == 66
    assert Kayrock.KafkaSchemaMetadata.api_key(66) == :list_transactions

    for version <- 0..2 do
      assert Kayrock.ListTransactions.get_request_struct(version).__struct__.api_vsn() == version
    end
  end

  # state_filters ["Ongoing"], producer_id_filters [5, 6]
  @filters <<2, 8, "Ongoing", 3, 5::64, 6::64>>

  test "v0 serializes a request" do
    request = %Kayrock.ListTransactions.V0.Request{
      correlation_id: 1,
      client_id: "kayrock",
      state_filters: ["Ongoing"],
      producer_id_filters: [5, 6]
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<@api_key::16, 0::16, 1::32, 7::16, "kayrock", 0>> <> @filters <> <<0>>
  end

  test "v1 serializes a request with a duration filter" do
    request = %Kayrock.ListTransactions.V1.Request{
      correlation_id: 2,
      client_id: "kayrock",
      state_filters: ["Ongoing"],
      producer_id_filters: [5, 6],
      duration_filter: 30_000
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<@api_key::16, 1::16, 2::32, 7::16, "kayrock", 0>> <>
               @filters <> <<30_000::64, 0>>
  end

  test "v2 serializes a request with a transactional id pattern" do
    request = %Kayrock.ListTransactions.V2.Request{
      correlation_id: 3,
      client_id: "kayrock",
      state_filters: ["Ongoing"],
      producer_id_filters: [5, 6],
      duration_filter: -1,
      transactional_id_pattern: "^app-.*"
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<@api_key::16, 2::16, 3::32, 7::16, "kayrock", 0>> <>
               @filters <> <<-1::64-signed, 8, "^app-.*", 0>>
  end

  test "v2 serializes a null pattern and empty filters" do
    request = %Kayrock.ListTransactions.V2.Request{
      correlation_id: 0,
      client_id: nil,
      state_filters: [],
      producer_id_filters: [],
      duration_filter: -1,
      transactional_id_pattern: nil
    }

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
             <<@api_key::16, 2::16, 0::32, -1::16-signed, 0, 1, 1, -1::64-signed, 0, 0>>
  end

  for version <- 0..2 do
    test "v#{version} deserializes a response" do
      version = unquote(version)
      module = Module.concat([Kayrock.ListTransactions, :"V#{version}", Response])

      binary =
        <<
          # correlation_id, header tag buffer
          7::32,
          0,
          # throttle_time_ms, error_code
          0::32,
          0::16,
          # unknown_state_filters: ["Bogus"]
          2,
          6,
          "Bogus",
          # transaction_states: 1
          2,
          4,
          "app",
          5::64,
          14,
          "PrepareCommit",
          0,
          # response tagged fields
          0
        >>

      assert Kayrock.ListTransactions.deserialize(version, binary) ==
               {%{
                  struct(module)
                  | correlation_id: 7,
                    throttle_time_ms: 0,
                    error_code: 0,
                    unknown_state_filters: ["Bogus"],
                    transaction_states: [
                      %{
                        transactional_id: "app",
                        producer_id: 5,
                        transaction_state: "PrepareCommit",
                        tagged_fields: []
                      }
                    ],
                    tagged_fields: []
                }, <<>>}
    end
  end
end
