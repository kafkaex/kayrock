defmodule Kayrock.JoinGroupV7V9Test do
  @moduledoc """
  JoinGroup (API key 11) v7-v9, generated from the Apache Kafka 4.4.0 JSON
  spec (`JoinGroupRequest.json` / `JoinGroupResponse.json`).

  Fixtures are derived by hand from the spec:

    * v7 request is identical to v6; the response adds a nullable
      `ProtocolType` and makes `ProtocolName` nullable (KIP-559);
    * v8 request adds a nullable `Reason` (KIP-800); response as v7;
    * v9 request is identical to v8; the response adds `SkipAssignment`.

  All three are flexible: request header v2 (client_id as int16 string plus
  a header tag buffer) and response header v1 (correlation_id plus a header
  tag buffer).
  """
  use ExUnit.Case, async: true

  import Kayrock.TestSupport

  alias Kayrock.JoinGroup

  @new_versions 7..9

  # Request header v2 for JoinGroup (11) at `version`, correlation_id 42,
  # client_id "kayrock".
  defp request_header(version) do
    <<0, 11, 0, version, 0, 0, 0, 42, 0, 7, "kayrock", 0>>
  end

  defp request(version, extra \\ %{}) do
    base = %{
      correlation_id: 42,
      client_id: "kayrock",
      group_id: "grp",
      session_timeout_ms: 45_000,
      rebalance_timeout_ms: 300_000,
      member_id: "",
      group_instance_id: nil,
      protocol_type: "consumer",
      protocols: [%{name: "range", metadata: <<0, 1>>, tagged_fields: []}],
      tagged_fields: []
    }

    struct!(Module.concat([JoinGroup, :"V#{version}", Request]), Map.merge(base, extra))
  end

  # Request body shared by v6-v9 up to and including `protocols`.
  @request_body_v7 <<
    # group_id: compact string "grp" (len+1 = 4)
    4,
    "grp",
    # session_timeout_ms: 45_000
    0,
    0,
    175,
    200,
    # rebalance_timeout_ms: 300_000
    0,
    4,
    147,
    224,
    # member_id: compact string "" (len+1 = 1)
    1,
    # group_instance_id: compact nullable string null
    0,
    # protocol_type: compact string "consumer" (len+1 = 9)
    9,
    "consumer",
    # protocols: compact array, 1 element (len+1 = 2)
    2,
    # protocols[0].name: "range"
    6,
    "range",
    # protocols[0].metadata: compact bytes <<0, 1>> (len+1 = 3)
    3,
    0,
    1,
    # protocols[0] tagged fields
    0
  >>

  describe "request serialization" do
    test "v7 matches the spec layout (same body as v6)" do
      expected = request_header(7) <> @request_body_v7 <> <<0>>
      assert IO.iodata_to_binary(Kayrock.Request.serialize(request(7))) == expected
    end

    for version <- 8..9 do
      test "v#{version} appends a null reason" do
        version = unquote(version)
        # reason: compact nullable string null; then request tagged fields
        expected = request_header(version) <> @request_body_v7 <> <<0, 0>>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request(version))) == expected
      end

      test "v#{version} serializes a reason and a static member id" do
        version = unquote(version)
        req = request(version, %{group_instance_id: "inst-1", reason: "rejoin"})

        expected =
          request_header(version) <>
            <<4, "grp", 0, 0, 175, 200, 0, 4, 147, 224, 1>> <>
            <<7, "inst-1">> <>
            <<9, "consumer", 2, 6, "range", 3, 0, 1, 0>> <>
            <<7, "rejoin">> <>
            <<0>>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(req)) == expected
      end
    end

    test "v9 serializes GroupProtocolMetadata member metadata" do
      metadata = %Kayrock.GroupProtocolMetadata{version: 0, topics: ["t"], user_data: ""}

      req =
        request(9, %{protocols: [%{name: "range", metadata: metadata, tagged_fields: []}]})

      serialized = IO.iodata_to_binary(Kayrock.Request.serialize(req))

      # GroupProtocolMetadata: version int16 0, topics array [1 x "t"],
      # user_data bytes "" (int32 length 0) = 13 bytes, compact len+1 = 14
      metadata_bytes = <<0, 0, 0, 0, 0, 1, 0, 1, "t", 0, 0, 0, 0>>
      assert byte_size(metadata_bytes) == 13

      expected =
        request_header(9) <>
          <<4, "grp", 0, 0, 175, 200, 0, 4, 147, 224, 1, 0, 9, "consumer">> <>
          <<2, 6, "range", 14>> <> metadata_bytes <> <<0>> <> <<0, 0>>

      assert serialized == expected
    end

    test "request structs report their version and api key" do
      for version <- @new_versions do
        req = request(version)
        assert Kayrock.Request.api_vsn(req) == version
        <<11::16, ^version::16, _::binary>> = IO.iodata_to_binary(Kayrock.Request.serialize(req))
        assert JoinGroup.get_request_struct(version).__struct__ == req.__struct__
      end
    end
  end

  # Response body for v7/v8: a successful join of a single leader member.
  defp response_v7_body do
    <<
      # throttle_time_ms: 0
      0,
      0,
      0,
      0,
      # error_code: 0
      0,
      0,
      # generation_id: 3
      0,
      0,
      0,
      3,
      # protocol_type: compact nullable string "consumer"
      9,
      "consumer",
      # protocol_name: compact nullable string "range"
      6,
      "range",
      # leader: "m-1"
      4,
      "m-1"
    >>
  end

  defp response_tail do
    <<
      # member_id: "m-1"
      4,
      "m-1",
      # members: compact array, 1 element
      2,
      # members[0].member_id: "m-1"
      4,
      "m-1",
      # members[0].group_instance_id: null
      0,
      # members[0].metadata: compact bytes <<9, 9>>
      3,
      9,
      9,
      # members[0] tagged fields
      0,
      # response tagged fields
      0
    >>
  end

  defp expected_response(version, extra \\ %{}) do
    base = %{
      correlation_id: 42,
      throttle_time_ms: 0,
      error_code: 0,
      generation_id: 3,
      protocol_type: "consumer",
      protocol_name: "range",
      leader: "m-1",
      member_id: "m-1",
      members: [
        %{member_id: "m-1", group_instance_id: nil, metadata: <<9, 9>>, tagged_fields: []}
      ],
      tagged_fields: []
    }

    struct!(Module.concat([JoinGroup, :"V#{version}", Response]), Map.merge(base, extra))
  end

  # Response header v1: correlation_id 42 + header tag buffer
  @response_header <<0, 0, 0, 42, 0>>

  defp response_binary(9, skip_assignment) do
    @response_header <> response_v7_body() <> <<skip_assignment>> <> response_tail()
  end

  defp response_binary(version, _) when version in 7..8 do
    @response_header <> response_v7_body() <> response_tail()
  end

  describe "response deserialization" do
    for version <- 7..8 do
      test "v#{version} decodes protocol_type and protocol_name" do
        version = unquote(version)
        module = Module.concat([JoinGroup, :"V#{version}", Response])

        assert {expected_response(version), <<>>} ==
                 module.deserialize(response_binary(version, nil))
      end
    end

    # Kayrock decodes booleans as the raw int8 (0 / 1), as for every other
    # boolean field.
    test "v9 decodes skip_assignment" do
      assert {expected_response(9, %{skip_assignment: 0}), <<>>} ==
               JoinGroup.V9.Response.deserialize(response_binary(9, 0))

      assert {expected_response(9, %{skip_assignment: 1}), <<>>} ==
               JoinGroup.V9.Response.deserialize(response_binary(9, 1))
    end

    test "v7 decodes null protocol_type / protocol_name (error response)" do
      # error_code 79 MEMBER_ID_REQUIRED, generation_id -1
      # protocol_type null, protocol_name null, leader "", member_id "m-2"
      # no members, no tagged fields
      binary =
        @response_header <>
          <<0, 0, 0, 0>> <>
          <<0, 79, 255, 255, 255, 255>> <>
          <<0, 0, 1, 4, "m-2">> <>
          <<1, 0>>

      {response, <<>>} = JoinGroup.V7.Response.deserialize(binary)
      assert response.error_code == 79
      assert response.generation_id == -1
      assert response.protocol_type == nil
      assert response.protocol_name == nil
      assert response.leader == ""
      assert response.member_id == "m-2"
      assert response.members == []
    end

    test "Kayrock.JoinGroup.deserialize/2 dispatches the new versions" do
      for version <- @new_versions do
        assert {%{__struct__: struct}, <<>>} =
                 JoinGroup.deserialize(version, response_binary(version, 0))

        assert struct == Module.concat([JoinGroup, :"V#{version}", Response])
      end
    end

    test "truncated responses raise and trailing bytes are returned" do
      for version <- @new_versions do
        module = Module.concat([JoinGroup, :"V#{version}", Response])
        binary = response_binary(version, 0)

        for truncate_at <- truncation_points(binary) do
          assert_truncated_error(module, binary, truncate_at)
        end

        assert_extra_bytes_returned(module, binary, <<1, 2, 3>>)
      end
    end
  end

  test "version metadata" do
    assert JoinGroup.max_vsn() == 9
    assert Kayrock.KafkaSchemaMetadata.version_range(:join_group) == {0, 9}
  end
end
