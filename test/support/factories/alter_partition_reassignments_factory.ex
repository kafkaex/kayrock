defmodule Kayrock.Test.Factories.AlterPartitionReassignmentsFactory do
  @moduledoc """
  Factory for AlterPartitionReassignments API test data (V0).

  API Key: 45
  Used to: Alter partition reassignments (move replicas between brokers).

  Protocol structure:
  - V0: Uses flexible/compact format (all versions use compact format)
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.AlterPartitionReassignments.V0.Request{
      correlation_id: 0,
      client_id: "test",
      timeout_ms: 30_000,
      topics: [
        %{
          name: "topic1",
          partitions: [
            %{partition_index: 0, replicas: [0, 1, 2], tagged_fields: []}
          ],
          tagged_fields: []
        }
      ],
      tagged_fields: []
    }

    # Captured from actual serialization
    expected_binary = <<
      0,
      45,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      4,
      116,
      101,
      115,
      116,
      0,
      0,
      0,
      117,
      48,
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
      4,
      0,
      0,
      0,
      0,
      0,
      0,
      0,
      1,
      0,
      0,
      0,
      2,
      0,
      0,
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
      # HEADER tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # error_message: compact_nullable_string (null = 0)
      0,
      # responses compact_array length+1 = 2 (1 element)
      2,
      # name: compact_string length+1 = 7
      7,
      "topic1"::binary,
      # partitions compact_array length+1 = 2 (1 element)
      2,
      # partition_index
      0,
      0,
      0,
      0,
      # error_code
      0,
      0,
      # error_message (null = 0)
      0,
      # tagged_fields for partition
      0,
      # tagged_fields for response entry
      0,
      # tagged_fields for root
      0
    >>

    expected_struct = %Kayrock.AlterPartitionReassignments.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      error_code: 0,
      error_message: nil,
      responses: [
        %{
          name: "topic1",
          tagged_fields: [],
          partitions: [
            %{
              partition_index: 0,
              error_code: 0,
              error_message: nil,
              tagged_fields: []
            }
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

  def error_response(0, opts \\ []) do
    error_code = Keyword.get(opts, :error_code, 58)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Not controller"
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
      error_code::16-signed,
      # error_message: compact_string
      error_message_len_plus_one::8,
      error_message::binary,
      # responses compact_array (empty = 1)
      1,
      # tagged_fields for root
      0
    >>
  end
end
