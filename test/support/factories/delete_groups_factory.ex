defmodule Kayrock.Test.Factories.DeleteGroupsFactory do
  @moduledoc """
  Factory for DeleteGroups API test data (V0-V2).

  API Key: 42
  Used to: Delete consumer groups from the Kafka cluster.

  Protocol structure:
  - V0-V1: Basic delete with throttle_time_ms in response
  - V2: Flexible/compact format with tagged_fields
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.DeleteGroups.V0.Request{
      correlation_id: 0,
      client_id: "test",
      groups_names: ["group1"]
    }

    expected_binary = <<
      # api_key
      0,
      42,
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
      # groups_names array length
      0,
      0,
      0,
      1,
      # group1
      0,
      6,
      "group1"::binary
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DeleteGroups.V1.Request{
      correlation_id: 1,
      client_id: "test",
      groups_names: ["group1"]
    }

    expected_binary = <<
      # api_key
      0,
      42,
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
      # groups_names array length
      0,
      0,
      0,
      1,
      # group1
      0,
      6,
      "group1"::binary
    >>

    {request, expected_binary}
  end

  def request_data(2) do
    request = %Kayrock.DeleteGroups.V2.Request{
      correlation_id: 2,
      client_id: "test",
      groups_names: ["group1"],
      tagged_fields: []
    }

    expected_binary = <<
      # api_key
      0,
      42,
      # api_version
      0,
      2,
      # correlation_id
      0,
      0,
      0,
      2,
      # client_id
      0,
      4,
      "test"::binary,
      # header tagged_fields (empty)
      0,
      # groups_names compact array (length + 1 = 2)
      2,
      # group1 compact string (length + 1 = 7)
      7,
      "group1"::binary,
      # tagged_fields (empty)
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
      # results array length
      0,
      0,
      0,
      1,
      # group_id
      0,
      6,
      "group1"::binary,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteGroups.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      results: [%{group_id: "group1", error_code: 0}]
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
      # results array length
      0,
      0,
      0,
      1,
      # group_id
      0,
      6,
      "group1"::binary,
      # error_code
      0,
      0
    >>

    expected_struct = %Kayrock.DeleteGroups.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      results: [%{group_id: "group1", error_code: 0}]
    }

    {binary, expected_struct}
  end

  def response_data(2) do
    binary = <<
      # correlation_id
      0,
      0,
      0,
      2,
      # header tagged_fields (empty)
      0,
      # throttle_time_ms
      0,
      0,
      0,
      100,
      # results compact array (length + 1 = 2)
      2,
      # group_id compact string (length + 1 = 7)
      7,
      "group1"::binary,
      # error_code
      0,
      0,
      # result tagged_fields (empty)
      0,
      # response tagged_fields (empty)
      0
    >>

    expected_struct = %Kayrock.DeleteGroups.V2.Response{
      correlation_id: 2,
      throttle_time_ms: 100,
      results: [%{group_id: "group1", error_code: 0, tagged_fields: []}],
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
    error_code = Keyword.get(opts, :error_code, 69)
    correlation_id = Keyword.get(opts, :correlation_id, 0)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # results array length
      0,
      0,
      0,
      1,
      # group_id
      0,
      6,
      "group1"::binary,
      # error_code
      error_code::16-signed
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 69)
    correlation_id = Keyword.get(opts, :correlation_id, 1)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # results array length
      0,
      0,
      0,
      1,
      # group_id
      0,
      6,
      "group1"::binary,
      # error_code
      error_code::16-signed
    >>
  end

  def error_response(2, opts) do
    error_code = Keyword.get(opts, :error_code, 69)
    correlation_id = Keyword.get(opts, :correlation_id, 2)

    <<
      correlation_id::32,
      # header tagged_fields
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # results compact array (length + 1 = 2)
      2,
      # group_id compact string
      7,
      "group1"::binary,
      # error_code
      error_code::16-signed,
      # result tagged_fields
      0,
      # response tagged_fields
      0
    >>
  end

  def multi_group_response(0) do
    <<
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
      # results array length (2)
      0,
      0,
      0,
      2,
      # group1 - success
      0,
      6,
      "group1"::binary,
      0,
      0,
      # group2 - error (GROUP_ID_NOT_FOUND = 69)
      0,
      6,
      "group2"::binary,
      0,
      69
    >>
  end

  def multi_group_response(1), do: multi_group_response(0)

  def multi_group_response(2) do
    <<
      # correlation_id
      0,
      0,
      0,
      2,
      # header tagged_fields
      0,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # results compact array (length + 1 = 3)
      3,
      # group1 - success
      7,
      "group1"::binary,
      0,
      0,
      0,
      # group2 - error
      7,
      "group2"::binary,
      0,
      69,
      0,
      # response tagged_fields
      0
    >>
  end
end
