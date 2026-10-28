defmodule Kayrock.ListGroupsV4V5Test do
  @moduledoc """
  ListGroups (API key 16) v4-v5, generated from the Apache Kafka 4.4.0 JSON
  spec.

    * v4 adds the request `StatesFilter` and the per-group `GroupState`
      (KIP-518);
    * v5 adds the request `TypesFilter` and the per-group `GroupType`
      (KIP-848).

  Flexible: request header v2, response header v1. Fixtures are derived by
  hand from `ListGroupsRequest.json` / `ListGroupsResponse.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.ListGroups.V4
  alias Kayrock.ListGroups.V5

  defp header(version), do: <<0, 16, 0, version, 0, 0, 0, 3, 0, 7, "kayrock", 0>>

  describe "request serialization" do
    test "v4 serializes an empty states filter" do
      request = %V4.Request{correlation_id: 3, client_id: "kayrock", states_filter: []}

      # states_filter: empty compact array (1), tagged fields (0)
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == header(4) <> <<1, 0>>
    end

    test "v4 serializes a states filter" do
      request = %V4.Request{
        correlation_id: 3,
        client_id: "kayrock",
        states_filter: ["Stable", "Empty"]
      }

      expected = header(4) <> <<3, 7, "Stable", 6, "Empty", 0>>
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v5 serializes empty filters" do
      request = %V5.Request{correlation_id: 3, client_id: "kayrock"}

      # states_filter [], types_filter [], tagged fields
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               header(5) <> <<1, 1, 0>>
    end

    test "v5 serializes states and types filters" do
      request = %V5.Request{
        correlation_id: 3,
        client_id: "kayrock",
        states_filter: ["Stable"],
        types_filter: ["classic", "consumer"]
      }

      expected =
        header(5) <>
          <<
            # states_filter ["Stable"]
            2,
            7,
            "Stable",
            # types_filter ["classic", "consumer"]
            3,
            8,
            "classic",
            9,
            "consumer",
            # tagged fields
            0
          >>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      assert Kayrock.Request.api_vsn(request) == 5
    end
  end

  # correlation_id 3, header tag buffer, throttle_time_ms 0, error_code 0
  @response_prefix <<0, 0, 0, 3, 0, 0, 0, 0, 0, 0, 0>>

  @v4_response @response_prefix <>
                 <<
                   # groups: 1 element
                   2,
                   # group_id "g1"
                   3,
                   "g1",
                   # protocol_type "consumer"
                   9,
                   "consumer",
                   # group_state "Stable"
                   7,
                   "Stable",
                   # group tagged fields, response tagged fields
                   0,
                   0
                 >>

  @v5_response @response_prefix <>
                 <<
                   # groups: 2 elements
                   3,
                   # g1: protocol_type "consumer", state "Stable", type "classic"
                   3,
                   "g1",
                   9,
                   "consumer",
                   7,
                   "Stable",
                   8,
                   "classic",
                   0,
                   # g2: protocol_type "consumer", state "Empty", type "consumer"
                   3,
                   "g2",
                   9,
                   "consumer",
                   6,
                   "Empty",
                   9,
                   "consumer",
                   0,
                   # response tagged fields
                   0
                 >>

  describe "response deserialization" do
    test "v4 decodes group_state" do
      expected = %V4.Response{
        correlation_id: 3,
        throttle_time_ms: 0,
        error_code: 0,
        groups: [
          %{group_id: "g1", protocol_type: "consumer", group_state: "Stable", tagged_fields: []}
        ],
        tagged_fields: []
      }

      assert V4.Response.deserialize(@v4_response) == {expected, <<>>}
      assert Kayrock.ListGroups.deserialize(4, @v4_response) == {expected, <<>>}
    end

    test "v5 decodes group_state and group_type" do
      expected = %V5.Response{
        correlation_id: 3,
        throttle_time_ms: 0,
        error_code: 0,
        groups: [
          %{
            group_id: "g1",
            protocol_type: "consumer",
            group_state: "Stable",
            group_type: "classic",
            tagged_fields: []
          },
          %{
            group_id: "g2",
            protocol_type: "consumer",
            group_state: "Empty",
            group_type: "consumer",
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      assert V5.Response.deserialize(@v5_response) == {expected, <<>>}
      assert Kayrock.ListGroups.deserialize(5, @v5_response) == {expected, <<>>}
    end

    test "truncated responses raise and trailing bytes are returned" do
      for {module, binary} <- [{V4.Response, @v4_response}, {V5.Response, @v5_response}] do
        for truncate_at <- truncation_points(binary) do
          assert_truncated_error(module, binary, truncate_at)
        end

        assert_extra_bytes_returned(module, binary, <<1, 2, 3>>)
      end
    end
  end

  test "version metadata" do
    assert Kayrock.ListGroups.max_vsn() == 5
    assert Kayrock.KafkaSchemaMetadata.version_range(:list_groups) == {0, 5}
    assert %V4.Request{states_filter: []} = Kayrock.ListGroups.get_request_struct(4)
    assert %V5.Request{types_filter: []} = Kayrock.ListGroups.get_request_struct(5)
  end
end
