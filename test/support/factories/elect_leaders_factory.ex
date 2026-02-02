defmodule Kayrock.Test.Factories.ElectLeadersFactory do
  @moduledoc """
  Factory for ElectLeaders API test data (V0-V2).

  API Key: 43
  Used to: Trigger leader election for topic partitions.

  Protocol structure:
  - V0: Basic election
  - V1+: Adds election_type field
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.ElectLeaders.V0.Request{
      correlation_id: 0,
      client_id: "test",
      topic_partitions: [
        %{topic: "topic1", partition_id: [0, 1]}
      ],
      timeout_ms: 30_000
    }

    expected_binary = <<
      # api_key
      0,
      43,
      # api_version
      0,
      0,
      # correlation_id
      0,
      0,
      0,
      0,
      # client_id
      0,
      4,
      "test"::binary,
      # topic_partitions array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_id array length
      0,
      0,
      0,
      2,
      # partition 0
      0,
      0,
      0,
      0,
      # partition 1
      0,
      0,
      0,
      1,
      # timeout_ms (30000)
      0,
      0,
      117,
      48
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.ElectLeaders.V1.Request{
      correlation_id: 1,
      client_id: "test",
      election_type: 0,
      topic_partitions: [
        %{topic: "topic1", partition_id: [0]}
      ],
      timeout_ms: 60_000
    }

    expected_binary = <<
      # api_key
      0,
      43,
      # api_version
      0,
      1,
      # correlation_id
      0,
      0,
      0,
      1,
      # client_id
      0,
      4,
      "test"::binary,
      # election_type (0 = PREFERRED)
      0,
      # topic_partitions array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_id array length
      0,
      0,
      0,
      1,
      # partition 0
      0,
      0,
      0,
      0,
      # timeout_ms (60000)
      0,
      0,
      234,
      96
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.ElectLeaders.V2.Request{
      correlation_id: 2,
      client_id: "test",
      election_type: 1,
      topic_partitions: [
        %{topic: "topic1", partition_id: [0], tagged_fields: []}
      ],
      timeout_ms: 30_000,
      tagged_fields: []
    }

    # V2 uses flexible/compact format
    # Captured from actual serialization
    expected_binary = <<
      0,
      43,
      0,
      2,
      0,
      0,
      0,
      2,
      0,
      4,
      116,
      101,
      115,
      116,
      0,
      1,
      2,
      7,
      116,
      111,
      112,
      105,
      99,
      49,
      2,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      117,
      48,
      0
    >>

    {request, expected_binary}
  end

  # ============================================
  # Response Data (binary + expected struct)
  # ============================================

  def response_data(0) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # replica_election_results array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_result array length
      0,
      0,
      0,
      1,
      # partition_id
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.ElectLeaders.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      replica_election_results: [
        %{
          topic: "topic1",
          partition_result: [
            %{partition_id: 0, error_code: 0, error_message: nil}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(1) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      1,
      # throttle_time_ms
      0,
      0,
      0,
      50,
      # error_code (top-level for V1)
      0,
      0,
      # replica_election_results array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_result array length
      0,
      0,
      0,
      1,
      # partition_id
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255
    >>

    expected_struct = %Kayrock.ElectLeaders.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      error_code: 0,
      replica_election_results: [
        %{
          topic: "topic1",
          partition_result: [
            %{partition_id: 0, error_code: 0, error_message: nil}
          ]
        }
      ]
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    # V2 uses flexible/compact format with tagged_fields
    binary = <<
      # correlation_id
      0,
      0,
      0,
      2,
      # HEADER tagged_fields (empty = 0)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # error_code
      0,
      0,
      # replica_election_results compact_array length+1 = 2 (1 element)
      2,
      # topic: compact_string length+1 = 7
      7,
      "topic1"::binary,
      # partition_result: compact_array length+1 = 2 (1 element)
      2,
      # partition_id
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # error_message: compact_nullable_string (null = 0)
      0,
      # tagged_fields for partition_result (empty = 0)
      0,
      # tagged_fields for replica_election_results entry (empty = 0)
      0,
      # tagged_fields for root (empty = 0)
      0
    >>

    expected_struct = %Kayrock.ElectLeaders.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      error_code: 0,
      replica_election_results: [
        %{
          topic: "topic1",
          tagged_fields: [],
          partition_result: [
            %{partition_id: 0, error_code: 0, error_message: nil, tagged_fields: []}
          ]
        }
      ],
      tagged_fields: []
    }

    {binary, expected_struct}
  end

  # ============================================
  # Helper Functions
  # ============================================

  def captured_response_binary(version) do
    {binary, _struct} = response_data(version)
    binary
  end

  def error_response(version, opts \\ [])

  def error_response(0, opts) do
    error_code = Keyword.get(opts, :error_code, 84)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Election failed"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # replica_election_results array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_result array length
      0,
      0,
      0,
      1,
      # partition_id
      0,
      0,
      0,
      0,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 84)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = "Election failed"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # error_code (top-level for V1)
      0,
      0,
      # replica_election_results array length
      0,
      0,
      0,
      1,
      # topic
      0,
      6,
      "topic1"::binary,
      # partition_result array length
      0,
      0,
      0,
      1,
      # partition_id
      0,
      0,
      0,
      0,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary
    >>
  end

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 84)
    correlation_id = Keyword.get(opts, :correlation_id, 2)
    error_message = "Election failed"
    # compact_string length+1 encoding
    error_message_len_plus_one = byte_size(error_message) + 1

    <<
      correlation_id::32,
      # HEADER tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # top-level error_code
      0,
      0,
      # replica_election_results compact_array (1 element = length+1 = 2)
      2,
      # topic: compact_string length+1 = 7
      7,
      "topic1"::binary,
      # partition_result compact_array (1 element = 2)
      2,
      # partition_id
      0,
      0,
      0,
      0,
      error_code::16-signed,
      # error_message: compact_string
      error_message_len_plus_one::8,
      error_message::binary,
      # tagged_fields for partition_result
      0,
      # tagged_fields for replica_election_results entry
      0,
      # tagged_fields for root
      0
    >>
  end
end
