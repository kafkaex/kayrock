defmodule Kayrock.SyncGroupV5Test do
  @moduledoc """
  SyncGroup (API key 14) v5, generated from the Apache Kafka 4.4.0 JSON spec.

  v5 adds nullable `ProtocolType` and `ProtocolName` to both the request and
  the response (KIP-559). Flexible: request header v2, response header v1.
  Fixtures are derived by hand from `SyncGroupRequest.json` /
  `SyncGroupResponse.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.MemberAssignment
  alias Kayrock.MemberAssignment.PartitionAssignment
  alias Kayrock.SyncGroup.V5

  @header <<0, 14, 0, 5, 0, 0, 0, 9, 0, 7, "kayrock", 0>>

  # MemberAssignment{version 0, [{"t", [0]}], user_data ""}:
  # version int16, array int32 1, topic string "t", partitions array [0],
  # user_data bytes "" (int32 0)
  @assignment_bytes <<0, 0, 0, 0, 0, 1, 0, 1, "t", 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0>>

  defp request(assignment) do
    %V5.Request{
      correlation_id: 9,
      client_id: "kayrock",
      group_id: "grp",
      generation_id: 3,
      member_id: "m-1",
      group_instance_id: nil,
      protocol_type: "consumer",
      protocol_name: "range",
      assignments: [%{member_id: "m-1", assignment: assignment, tagged_fields: []}],
      tagged_fields: []
    }
  end

  defp expected_request(assignment_bytes) do
    @header <>
      <<
        # group_id "grp"
        4,
        "grp",
        # generation_id 3
        0,
        0,
        0,
        3,
        # member_id "m-1"
        4,
        "m-1",
        # group_instance_id null
        0,
        # protocol_type "consumer"
        9,
        "consumer",
        # protocol_name "range"
        6,
        "range",
        # assignments: 1 element, member_id "m-1"
        2,
        4,
        "m-1",
        # assignment: compact bytes
        byte_size(assignment_bytes) + 1
      >> <> assignment_bytes <> <<0, 0>>
  end

  @partition_assignment %MemberAssignment{
    version: 0,
    partition_assignments: [%PartitionAssignment{topic: "t", partitions: [0]}],
    user_data: ""
  }

  describe "request serialization" do
    test "v5 serializes raw assignment bytes" do
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request(<<1, 2>>))) ==
               expected_request(<<1, 2>>)
    end

    test "v5 serializes a MemberAssignment struct" do
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request(@partition_assignment))) ==
               expected_request(@assignment_bytes)
    end

    test "v5 serializes null protocol type and name" do
      req = %{request(<<>>) | protocol_type: nil, protocol_name: nil}

      expected =
        @header <> <<4, "grp", 0, 0, 0, 3, 4, "m-1", 0, 0, 0, 2, 4, "m-1", 1, 0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(req)) == expected
      assert Kayrock.Request.api_vsn(req) == 5
    end
  end

  defp response_binary(assignment_bytes) do
    <<
      # correlation_id 9 + header tag buffer
      0,
      0,
      0,
      9,
      0,
      # throttle_time_ms 0
      0,
      0,
      0,
      0,
      # error_code 0
      0,
      0,
      # protocol_type "consumer"
      9,
      "consumer",
      # protocol_name "range"
      6,
      "range",
      # assignment: compact bytes
      byte_size(assignment_bytes) + 1
    >> <> assignment_bytes <> <<0>>
  end

  describe "response deserialization" do
    test "v5 decodes protocol type/name and the member assignment" do
      binary = response_binary(@assignment_bytes)

      expected = %V5.Response{
        correlation_id: 9,
        throttle_time_ms: 0,
        error_code: 0,
        protocol_type: "consumer",
        protocol_name: "range",
        assignment: @partition_assignment,
        tagged_fields: []
      }

      assert V5.Response.deserialize(binary) == {expected, <<>>}
      assert Kayrock.SyncGroup.deserialize(5, binary) == {expected, <<>>}
    end

    # Kayrock.MemberAssignment (shared with every SyncGroup version) builds
    # topic and partition lists in reverse wire order; pinned here so v5 is
    # known to behave exactly like v0-v4.
    test "v5 decodes a multi-partition assignment like earlier versions" do
      assignment = %MemberAssignment{
        version: 0,
        partition_assignments: [%PartitionAssignment{topic: "t", partitions: [0, 1, 2]}],
        user_data: ""
      }

      bytes = IO.iodata_to_binary(MemberAssignment.serialize(assignment))
      {v5, <<>>} = V5.Response.deserialize(response_binary(bytes))

      v4_binary =
        <<0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 0, byte_size(bytes) + 1>> <> bytes <> <<0>>

      {v4, <<>>} = Kayrock.SyncGroup.V4.Response.deserialize(v4_binary)

      assert v5.assignment == v4.assignment

      assert [%PartitionAssignment{topic: "t", partitions: [2, 1, 0]}] =
               v5.assignment.partition_assignments
    end

    test "v5 decodes an empty assignment (error response)" do
      binary = <<0, 0, 0, 9, 0, 0, 0, 0, 0, 0, 27, 0, 0, 1, 0>>
      {response, <<>>} = V5.Response.deserialize(binary)
      assert response.error_code == 27
      assert response.protocol_type == nil
      assert response.protocol_name == nil
      assert response.assignment == %MemberAssignment{}
    end

    test "v5 truncated responses raise and trailing bytes are returned" do
      binary = response_binary(@assignment_bytes)

      for truncate_at <- truncation_points(binary) do
        assert_truncated_error(V5.Response, binary, truncate_at)
      end

      assert_extra_bytes_returned(V5.Response, binary, <<1, 2, 3>>)
    end
  end

  test "version metadata" do
    assert Kayrock.SyncGroup.max_vsn() == 5
    assert Kayrock.KafkaSchemaMetadata.version_range(:sync_group) == {0, 5}
    assert %V5.Request{} = Kayrock.SyncGroup.get_request_struct(5)
  end
end
