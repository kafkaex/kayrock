defmodule Kayrock.LeaveGroupV5Test do
  @moduledoc """
  LeaveGroup (API key 13) v5, generated from the Apache Kafka 4.4.0 JSON spec.

  v5 adds a nullable `Reason` to each leaving member (KIP-800); the response
  is identical to v4. Flexible: request header v2, response header v1.
  Fixtures are derived by hand from `LeaveGroupRequest.json` /
  `LeaveGroupResponse.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.LeaveGroup.V5

  @header <<0, 13, 0, 5, 0, 0, 0, 7, 0, 7, "kayrock", 0>>

  describe "request serialization" do
    test "v5 serializes members with and without a reason" do
      request = %V5.Request{
        correlation_id: 7,
        client_id: "kayrock",
        group_id: "grp",
        members: [
          %{member_id: "m-1", group_instance_id: nil, reason: "shutdown", tagged_fields: []},
          %{member_id: "", group_instance_id: "inst-2", reason: nil, tagged_fields: []}
        ],
        tagged_fields: []
      }

      expected =
        @header <>
          <<
            # group_id "grp"
            4,
            "grp",
            # members: 2 elements
            3,
            # members[0]: member_id "m-1", group_instance_id null, reason "shutdown"
            4,
            "m-1",
            0,
            9,
            "shutdown",
            0,
            # members[1]: member_id "", group_instance_id "inst-2", reason null
            1,
            7,
            "inst-2",
            0,
            0,
            # request tagged fields
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      assert Kayrock.Request.api_vsn(request) == 5
    end

    test "get_request_struct/1 returns v5" do
      assert %V5.Request{group_id: "", members: []} = Kayrock.LeaveGroup.get_request_struct(5)
    end
  end

  @response <<
    # correlation_id 7 + header tag buffer
    0,
    0,
    0,
    7,
    0,
    # throttle_time_ms 0
    0,
    0,
    0,
    0,
    # error_code 0
    0,
    0,
    # members: 1 element
    2,
    # member_id "m-1", group_instance_id null, error_code 25 (UNKNOWN_MEMBER_ID)
    4,
    "m-1",
    0,
    0,
    25,
    0,
    # response tagged fields
    0
  >>

  describe "response deserialization" do
    test "v5 decodes per-member results" do
      expected = %V5.Response{
        correlation_id: 7,
        throttle_time_ms: 0,
        error_code: 0,
        members: [
          %{member_id: "m-1", group_instance_id: nil, error_code: 25, tagged_fields: []}
        ],
        tagged_fields: []
      }

      assert V5.Response.deserialize(@response) == {expected, <<>>}
      assert Kayrock.LeaveGroup.deserialize(5, @response) == {expected, <<>>}
    end

    test "v5 truncated responses raise and trailing bytes are returned" do
      for truncate_at <- truncation_points(@response) do
        assert_truncated_error(V5.Response, @response, truncate_at)
      end

      assert_extra_bytes_returned(V5.Response, @response, <<1, 2, 3>>)
    end
  end

  test "version metadata" do
    assert Kayrock.LeaveGroup.max_vsn() == 5
    assert Kayrock.KafkaSchemaMetadata.version_range(:leave_group) == {0, 5}
  end
end
