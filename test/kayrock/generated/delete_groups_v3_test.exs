defmodule Kayrock.DeleteGroupsV3Test do
  @moduledoc """
  DeleteGroups (API key 42) v3, generated from the Apache Kafka 4.4.0 JSON
  spec.

  The v3 request is identical to v2; the response adds a nullable per-group
  `ErrorMessage`. Flexible: request header v2, response header v1. Fixtures
  are derived by hand from `DeleteGroupsRequest.json` /
  `DeleteGroupsResponse.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.DeleteGroups.V3

  describe "request serialization" do
    test "v3 serializes the group names" do
      request = %V3.Request{
        correlation_id: 11,
        client_id: "kayrock",
        groups_names: ["g1", "g2"],
        tagged_fields: []
      }

      expected = <<
        # api_key 42, api_version 3, correlation_id 11, client_id "kayrock"
        0,
        42,
        0,
        3,
        0,
        0,
        0,
        11,
        0,
        7,
        "kayrock",
        # header tag buffer
        0,
        # groups_names: 2 compact strings
        3,
        3,
        "g1",
        3,
        "g2",
        # tagged fields
        0
      >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      assert Kayrock.Request.api_vsn(request) == 3
    end
  end

  @response <<
    # correlation_id 11 + header tag buffer
    0,
    0,
    0,
    11,
    0,
    # throttle_time_ms 0
    0,
    0,
    0,
    0,
    # results: 2 elements
    3,
    # g1: deleted, error_message null
    3,
    "g1",
    0,
    0,
    0,
    0,
    # g2: NON_EMPTY_GROUP (68) with a message
    3,
    "g2",
    0,
    68,
    17,
    "The group is not",
    0,
    # response tagged fields
    0
  >>

  describe "response deserialization" do
    test "v3 decodes per-group error messages" do
      expected = %V3.Response{
        correlation_id: 11,
        throttle_time_ms: 0,
        results: [
          %{group_id: "g1", error_code: 0, error_message: nil, tagged_fields: []},
          %{group_id: "g2", error_code: 68, error_message: "The group is not", tagged_fields: []}
        ],
        tagged_fields: []
      }

      assert V3.Response.deserialize(@response) == {expected, <<>>}
      assert Kayrock.DeleteGroups.deserialize(3, @response) == {expected, <<>>}
    end

    test "v3 truncated responses raise and trailing bytes are returned" do
      for truncate_at <- truncation_points(@response) do
        assert_truncated_error(V3.Response, @response, truncate_at)
      end

      assert_extra_bytes_returned(V3.Response, @response, <<1, 2, 3>>)
    end
  end

  test "version metadata" do
    assert Kayrock.DeleteGroups.max_vsn() == 3
    assert Kayrock.KafkaSchemaMetadata.version_range(:delete_groups) == {0, 3}
    assert %V3.Request{groups_names: []} = Kayrock.DeleteGroups.get_request_struct(3)
  end
end
