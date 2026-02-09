defmodule Kayrock.Test.Factories.DeleteAclsFactory do
  @moduledoc """
  Factory for DeleteAcls API test data (V0-V1).

  API Key: 31
  Used to: Delete ACLs matching filter criteria.

  Protocol structure:
  - V0: filters array with resource_type, resource_name, principal, host, operation, permission_type
  - V1: Adds resource_pattern_type_filter to filters
  """

  # ============================================
  # Request Data (struct + expected binary)
  # ============================================

  def request_data(0) do
    request = %Kayrock.DeleteAcls.V0.Request{
      correlation_id: 0,
      client_id: "test",
      filters: [
        %{
          resource_type: 2,
          resource_name: "topic1",
          principal: "User:alice",
          host: "*",
          operation: 2,
          permission_type: 3
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      31,
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
      # filters array length
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name (nullable_string)
      0,
      6,
      "topic1"::binary,
      # principal (nullable_string)
      0,
      10,
      "User:alice"::binary,
      # host (nullable_string)
      0,
      1,
      "*"::binary,
      # operation (2 = WRITE)
      2,
      # permission_type (3 = ALLOW)
      3
    >>

    {request, expected_binary}
  end

  def request_data(1) do
    request = %Kayrock.DeleteAcls.V1.Request{
      correlation_id: 1,
      client_id: "test",
      filters: [
        %{
          resource_type: 2,
          resource_name: "topic1",
          resource_pattern_type_filter: 3,
          principal: "User:bob",
          host: "*",
          operation: 3,
          permission_type: 3
        }
      ]
    }

    expected_binary = <<
      # api_key
      0,
      31,
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
      # filters array length
      0,
      0,
      0,
      1,
      # resource_type (2 = TOPIC)
      2,
      # resource_name (nullable_string)
      0,
      6,
      "topic1"::binary,
      # resource_pattern_type_filter (3 = PREFIXED)
      3,
      # principal (nullable_string)
      0,
      8,
      "User:bob"::binary,
      # host (nullable_string)
      0,
      1,
      "*"::binary,
      # operation (3 = READ)
      3,
      # permission_type (3 = ALLOW)
      3
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
      # filter_responses array length
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255,
      # matching_acls array length
      0,
      0,
      0,
      1,
      # matching_acl error_code
      0,
      0,
      # matching_acl error_message (null)
      255,
      255,
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary,
      # principal
      0,
      10,
      "User:alice"::binary,
      # host
      0,
      1,
      "*"::binary,
      # operation
      2,
      # permission_type
      3
    >>

    expected_struct = %Kayrock.DeleteAcls.V0.Response{
      correlation_id: 0,
      throttle_time_ms: 0,
      filter_responses: [
        %{
          error_code: 0,
          error_message: nil,
          matching_acls: [
            %{
              error_code: 0,
              error_message: nil,
              resource_type: 2,
              resource_name: "topic1",
              principal: "User:alice",
              host: "*",
              operation: 2,
              permission_type: 3
            }
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
      # filter_responses array length
      0,
      0,
      0,
      1,
      # error_code
      0,
      0,
      # error_message (null)
      255,
      255,
      # matching_acls array length
      0,
      0,
      0,
      1,
      # matching_acl error_code
      0,
      0,
      # matching_acl error_message (null)
      255,
      255,
      # resource_type
      2,
      # resource_name
      0,
      6,
      "topic1"::binary,
      # resource_pattern_type
      3,
      # principal
      0,
      8,
      "User:bob"::binary,
      # host
      0,
      1,
      "*"::binary,
      # operation
      3,
      # permission_type
      3
    >>

    expected_struct = %Kayrock.DeleteAcls.V1.Response{
      correlation_id: 1,
      throttle_time_ms: 50,
      filter_responses: [
        %{
          error_code: 0,
          error_message: nil,
          matching_acls: [
            %{
              error_code: 0,
              error_message: nil,
              resource_type: 2,
              resource_name: "topic1",
              resource_pattern_type: 3,
              principal: "User:bob",
              host: "*",
              operation: 3,
              permission_type: 3
            }
          ]
        }
      ]
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
    error_code = Keyword.get(opts, :error_code, 7)
    correlation_id = Keyword.get(opts, :correlation_id, 0)
    error_message = "Delete ACL failed"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # filter_responses array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary,
      # matching_acls array (empty)
      0,
      0,
      0,
      0
    >>
  end

  def error_response(1, opts) do
    error_code = Keyword.get(opts, :error_code, 7)
    correlation_id = Keyword.get(opts, :correlation_id, 1)
    error_message = "Delete ACL failed"
    error_message_len = byte_size(error_message)

    <<
      correlation_id::32,
      # throttle_time_ms
      0,
      0,
      0,
      0,
      # filter_responses array length
      0,
      0,
      0,
      1,
      error_code::16-signed,
      error_message_len::16,
      error_message::binary,
      # matching_acls array (empty)
      0,
      0,
      0,
      0
    >>
  end
end
