defmodule Kayrock.FlexibleHeaderTest do
  @moduledoc """
  Tests that request headers are correctly encoded per the Kafka protocol spec.

  Key insight: client_id uses int16-prefixed nullable_string in ALL header
  versions (v1 and v2). The Kafka RequestHeader.json defines ClientId with
  "flexibleVersions": "none". The ONLY difference between header v1 (non-flexible)
  and header v2 (flexible) is the tag_buffer <<0>> appended after client_id.
  """
  use ExUnit.Case, async: true

  describe "flexible version request headers (header v2)" do
    test "Heartbeat V4 (flexible) uses int16-prefixed client_id + tag_buffer" do
      request = %Kayrock.Heartbeat.V4.Request{
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-1",
        group_instance_id: nil,
        tagged_fields: [],
        correlation_id: 42,
        client_id: "kafka_ex"
      }

      serialized = IO.iodata_to_binary(Kayrock.Heartbeat.V4.Request.serialize(request))

      <<api_key::16, api_version::16, correlation_id::32, rest::binary>> = serialized

      assert api_key == 12
      assert api_version == 4
      assert correlation_id == 42

      # Header v2: int16-prefixed client_id (NOT compact), then tag_buffer <<0>>
      # "kafka_ex" (8 bytes) -> <<0, 8>> (int16 length) then "kafka_ex" then <<0>> tag_buffer
      assert <<0, 8, "kafka_ex", 0, _body::binary>> = rest
    end

    test "FindCoordinator V3 (flexible) uses int16-prefixed client_id + tag_buffer" do
      request = %Kayrock.FindCoordinator.V3.Request{
        key: "my-group",
        key_type: 0,
        tagged_fields: [],
        correlation_id: 1,
        client_id: "test"
      }

      serialized = IO.iodata_to_binary(Kayrock.FindCoordinator.V3.Request.serialize(request))

      <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

      # "test" (4 bytes) -> <<0, 4>> int16 prefix, then "test", then <<0>> tag_buffer
      assert <<0, 4, "test", 0, _body::binary>> = rest
    end

    test "ApiVersions V3 (flexible) uses int16-prefixed client_id + tag_buffer" do
      request = %Kayrock.ApiVersions.V3.Request{
        client_software_name: "kafka_ex",
        client_software_version: "1.0.0",
        tagged_fields: [],
        correlation_id: 1,
        client_id: "kafka_ex"
      }

      serialized = IO.iodata_to_binary(Kayrock.ApiVersions.V3.Request.serialize(request))

      <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

      # "kafka_ex" (8 bytes) -> <<0, 8>> int16 prefix, then string, then <<0>> tag_buffer
      assert <<0, 8, "kafka_ex", 0, _body::binary>> = rest
    end

    test "flexible version with nil client_id serializes as nullable_string null" do
      request = %Kayrock.Heartbeat.V4.Request{
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-1",
        group_instance_id: nil,
        tagged_fields: [],
        correlation_id: 42,
        client_id: nil
      }

      serialized = IO.iodata_to_binary(Kayrock.Heartbeat.V4.Request.serialize(request))

      <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

      # null client_id: <<255, 255>> (-1 as int16-signed), then <<0>> tag_buffer
      assert <<255, 255, 0, _body::binary>> = rest
    end
  end

  describe "non-flexible request headers (header v1)" do
    test "Heartbeat V3 (non-flexible) uses int16-prefixed client_id, no tag_buffer" do
      request = %Kayrock.Heartbeat.V3.Request{
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-1",
        group_instance_id: nil,
        correlation_id: 42,
        client_id: "kafka_ex"
      }

      serialized = IO.iodata_to_binary(Kayrock.Heartbeat.V3.Request.serialize(request))

      <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

      # Non-flexible: <<0, 8>> int16 prefix, then "kafka_ex", then body (no tag_buffer)
      assert <<0, 8, "kafka_ex", _body::binary>> = rest
    end

    test "non-flexible version with nil client_id serializes as nullable_string null" do
      request = %Kayrock.Heartbeat.V3.Request{
        group_id: "test-group",
        generation_id: 1,
        member_id: "member-1",
        group_instance_id: nil,
        correlation_id: 42,
        client_id: nil
      }

      serialized = IO.iodata_to_binary(Kayrock.Heartbeat.V3.Request.serialize(request))

      <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

      # null client_id: <<255, 255>> (-1 as int16-signed), then body directly (no tag_buffer)
      assert <<255, 255, _body::binary>> = rest
    end
  end

  describe "client_id edge cases" do
    test "empty string client_id is distinct from nil (flexible)" do
      base = %Kayrock.Heartbeat.V4.Request{
        group_id: "g",
        generation_id: 1,
        member_id: "m",
        group_instance_id: nil,
        tagged_fields: [],
        correlation_id: 1
      }

      empty = IO.iodata_to_binary(Kayrock.Heartbeat.V4.Request.serialize(%{base | client_id: ""}))
      null = IO.iodata_to_binary(Kayrock.Heartbeat.V4.Request.serialize(%{base | client_id: nil}))

      <<_::64, empty_rest::binary>> = empty
      <<_::64, null_rest::binary>> = null

      # empty string: <<0, 0>> (int16 length = 0), then <<0>> tag_buffer
      assert <<0, 0, 0, _::binary>> = empty_rest
      # null: <<255, 255>> (-1 as int16-signed), then <<0>> tag_buffer
      assert <<255, 255, 0, _::binary>> = null_rest

      # They must differ
      assert empty != null
    end

    test "empty string client_id is distinct from nil (non-flexible)" do
      base = %Kayrock.Heartbeat.V3.Request{
        group_id: "g",
        generation_id: 1,
        member_id: "m",
        group_instance_id: nil,
        correlation_id: 1
      }

      empty = IO.iodata_to_binary(Kayrock.Heartbeat.V3.Request.serialize(%{base | client_id: ""}))
      null = IO.iodata_to_binary(Kayrock.Heartbeat.V3.Request.serialize(%{base | client_id: nil}))

      <<_::64, empty_rest::binary>> = empty
      <<_::64, null_rest::binary>> = null

      # empty string: <<0, 0>> (int16 length = 0), then body
      assert <<0, 0, _::binary>> = empty_rest
      # null: <<255, 255>> (-1 as int16-signed), then body
      assert <<255, 255, _::binary>> = null_rest

      assert empty != null
    end
  end

  describe "header format consistency across all flexible APIs" do
    # Every flexible-version request must use int16-prefixed client_id + tag_buffer.
    # This data-driven test verifies the pattern for all APIs.

    @flexible_requests [
      {Kayrock.ApiVersions.V3.Request,
       %{client_software_name: "k", client_software_version: "1", tagged_fields: []}},
      {Kayrock.Metadata.V9.Request,
       %{
         topics: nil,
         allow_auto_topic_creation: true,
         include_cluster_authorized_operations: false,
         include_topic_authorized_operations: false,
         tagged_fields: []
       }},
      {Kayrock.FindCoordinator.V3.Request, %{key: "g", key_type: 0, tagged_fields: []}},
      {Kayrock.Heartbeat.V4.Request,
       %{
         group_id: "g",
         generation_id: 1,
         member_id: "m",
         group_instance_id: nil,
         tagged_fields: []
       }},
      {Kayrock.JoinGroup.V6.Request,
       %{
         group_id: "g",
         session_timeout_ms: 10_000,
         rebalance_timeout_ms: 5000,
         member_id: "",
         protocol_type: "consumer",
         protocols: [],
         group_instance_id: nil,
         tagged_fields: []
       }},
      {Kayrock.SyncGroup.V4.Request,
       %{
         group_id: "g",
         generation_id: 1,
         member_id: "m",
         group_instance_id: nil,
         assignments: [],
         tagged_fields: []
       }},
      {Kayrock.LeaveGroup.V4.Request, %{group_id: "g", members: [], tagged_fields: []}},
      {Kayrock.OffsetFetch.V6.Request, %{group_id: "g", topics: [], tagged_fields: []}},
      {Kayrock.OffsetCommit.V8.Request,
       %{
         group_id: "g",
         generation_id: 1,
         member_id: "m",
         group_instance_id: nil,
         topics: [],
         tagged_fields: []
       }},
      {Kayrock.DescribeGroups.V5.Request,
       %{groups: [], include_authorized_operations: false, tagged_fields: []}},
      {Kayrock.CreateTopics.V5.Request,
       %{topics: [], timeout_ms: 5000, validate_only: false, tagged_fields: []}},
      {Kayrock.DeleteTopics.V4.Request, %{topic_names: [], timeout_ms: 5000, tagged_fields: []}}
    ]

    for {mod, fields} <- @flexible_requests do
      api_name = mod |> Module.split() |> Enum.at(1)
      version = mod |> Module.split() |> Enum.at(2) |> String.trim_leading("V")

      test "#{api_name} V#{version} uses int16 client_id + tag_buffer" do
        mod = unquote(mod)

        request =
          struct(
            mod,
            Map.merge(unquote(Macro.escape(fields)), %{
              correlation_id: 1,
              client_id: "cid"
            })
          )

        serialized = IO.iodata_to_binary(mod.serialize(request))

        <<_api_key::16, _api_version::16, _correlation_id::32, rest::binary>> = serialized

        # All flexible requests: int16 prefix for "cid" (3 bytes) = <<0, 3>>, then "cid", then <<0>>
        assert <<0, 3, "cid", 0, _body::binary>> = rest
      end
    end
  end

  describe "ApiVersionsResponse V3 header exception (KIP-511)" do
    test "V3 response deserializes without header tag_buffer" do
      # ApiVersionsResponse ALWAYS uses response header v0 (just correlation_id).
      # The body uses flexible encoding (compact arrays, tagged fields).
      response_binary =
        <<
          # correlation_id
          0,
          0,
          0,
          42,
          # error_code (0 = no error)
          0,
          0,
          # api_keys compact_array: varint(2+1) = 3, then 2 entries
          3,
          # Entry 1: Produce api_key=0, min=0, max=9
          0,
          0,
          0,
          0,
          0,
          9,
          # entry tagged_fields
          0,
          # Entry 2: Fetch api_key=1, min=0, max=13
          0,
          1,
          0,
          0,
          0,
          13,
          # entry tagged_fields
          0,
          # throttle_time_ms
          0,
          0,
          0,
          0,
          # response body tagged_fields
          0
        >>

      {response, <<>>} = Kayrock.ApiVersions.V3.Response.deserialize(response_binary)

      assert response.correlation_id == 42
      assert response.error_code == 0
      assert length(response.api_keys) == 2
      assert hd(response.api_keys).api_key == 0
    end

    test "V2 response (non-flexible) still works unchanged" do
      {binary, expected} = Kayrock.Test.Factories.ApiVersionsFactory.response_data(2)
      {actual, <<>>} = Kayrock.ApiVersions.V2.Response.deserialize(binary)
      assert actual == expected
    end
  end
end
