defmodule Kayrock.DescribeGroupsV6Test do
  @moduledoc """
  DescribeGroups (API key 15) v6, generated from the Apache Kafka 4.4.0 JSON
  spec.

  The v6 request is identical to v5. The response adds a nullable
  per-group `ErrorMessage`, and the broker returns GROUP_ID_NOT_FOUND (69)
  for unknown groups (KIP-1043). Flexible: request header v2, response
  header v1. Fixtures are derived by hand from `DescribeGroupsRequest.json` /
  `DescribeGroupsResponse.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.DescribeGroups.V6

  describe "request serialization" do
    test "v6 serializes the group list and authorized-operations flag" do
      request = %V6.Request{
        correlation_id: 5,
        client_id: "kayrock",
        groups: ["g1", "g2"],
        include_authorized_operations: true,
        tagged_fields: []
      }

      expected = <<
        # api_key 15, api_version 6, correlation_id 5, client_id "kayrock"
        0,
        15,
        0,
        6,
        0,
        0,
        0,
        5,
        0,
        7,
        "kayrock",
        # header tag buffer
        0,
        # groups: compact array of 2 compact strings
        3,
        3,
        "g1",
        3,
        "g2",
        # include_authorized_operations true
        1,
        # tagged fields
        0
      >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      assert Kayrock.Request.api_vsn(request) == 6
    end
  end

  @response <<
    # correlation_id 5 + header tag buffer
    0,
    0,
    0,
    5,
    0,
    # throttle_time_ms 0
    0,
    0,
    0,
    0,
    # groups: 2 elements
    3,
    # --- groups[0]: a stable group with one member
    # error_code 0
    0,
    0,
    # error_message null
    0,
    # group_id "g1"
    3,
    "g1",
    # group_state "Stable"
    7,
    "Stable",
    # protocol_type "consumer"
    9,
    "consumer",
    # protocol_data "range"
    6,
    "range",
    # members: 1 element
    2,
    # member_id "m-1"
    4,
    "m-1",
    # group_instance_id null
    0,
    # client_id "c"
    2,
    "c",
    # client_host "/h"
    3,
    "/h",
    # member_metadata <<1>>
    2,
    1,
    # member_assignment <<2>>
    2,
    2,
    # member tagged fields
    0,
    # authorized_operations -2147483648 (not requested)
    128,
    0,
    0,
    0,
    # group tagged fields
    0,
    # --- groups[1]: unknown group (GROUP_ID_NOT_FOUND = 69)
    0,
    69,
    # error_message "Group g2 not found."
    20,
    "Group g2 not found.",
    # group_id "g2"
    3,
    "g2",
    # group_state "Dead"
    5,
    "Dead",
    # protocol_type "", protocol_data ""
    1,
    1,
    # members: empty
    1,
    # authorized_operations -2147483648
    128,
    0,
    0,
    0,
    # group tagged fields
    0,
    # response tagged fields
    0
  >>

  describe "response deserialization" do
    test "v6 decodes error_message and members" do
      expected = %V6.Response{
        correlation_id: 5,
        throttle_time_ms: 0,
        groups: [
          %{
            error_code: 0,
            error_message: nil,
            group_id: "g1",
            group_state: "Stable",
            protocol_type: "consumer",
            protocol_data: "range",
            members: [
              %{
                member_id: "m-1",
                group_instance_id: nil,
                client_id: "c",
                client_host: "/h",
                member_metadata: <<1>>,
                member_assignment: <<2>>,
                tagged_fields: []
              }
            ],
            authorized_operations: -2_147_483_648,
            tagged_fields: []
          },
          %{
            error_code: 69,
            error_message: "Group g2 not found.",
            group_id: "g2",
            group_state: "Dead",
            protocol_type: "",
            protocol_data: "",
            members: [],
            authorized_operations: -2_147_483_648,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert V6.Response.deserialize(@response) == {expected, <<>>}
      assert Kayrock.DescribeGroups.deserialize(6, @response) == {expected, <<>>}
    end

    test "v6 truncated responses raise and trailing bytes are returned" do
      for truncate_at <- truncation_points(@response) do
        assert_truncated_error(V6.Response, @response, truncate_at)
      end

      assert_extra_bytes_returned(V6.Response, @response, <<1, 2, 3>>)
    end
  end

  test "version metadata" do
    assert Kayrock.DescribeGroups.max_vsn() == 6
    assert Kayrock.KafkaSchemaMetadata.version_range(:describe_groups) == {0, 6}
    assert %V6.Request{groups: []} = Kayrock.DescribeGroups.get_request_struct(6)
  end
end
