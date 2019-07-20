defmodule Kayrock.JoinGroupTest do
  import Kayrock.TestSupport

  use ExUnit.Case

  test "serialize request v0" do
    expect = <<
      # Preamble
      11::16,
      0::16,
      42::32,
      9::16,
      "client_id"::binary,
      # GroupId
      5::16,
      "group"::binary,
      # SessionTimeout
      3600::32,
      # MemberId
      9::16,
      "member_id"::binary,
      # ProtocolType
      8::16,
      "consumer"::binary,
      # GroupProtocols array size
      1::32,
      # Basic strategy, "roundrobin" has some restrictions
      6::16,
      "assign"::binary,
      # length of metadata
      32::32,
      # v0
      0::16,
      # Topics array
      2::32,
      9::16,
      "topic_one"::binary,
      9::16,
      "topic_two"::binary,
      # UserData
      0::32
    >>

    request = %Kayrock.JoinGroup.V0.Request{
      client_id: "client_id",
      correlation_id: 42,
      group_id: "group",
      group_protocols: [
        %{
          protocol_metadata: %Kayrock.GroupProtocolMetadata{topics: ["topic_one", "topic_two"]},
          protocol_name: "assign"
        }
      ],
      member_id: "member_id",
      protocol_type: "consumer",
      session_timeout: 3600
    }

    got = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    assert got == expect, compare_binaries(got, expect)
  end

  test "serialize request v0 - protocol metadata preserialized" do
    expect = <<
      # Preamble
      11::16,
      0::16,
      42::32,
      9::16,
      "client_id"::binary,
      # GroupId
      5::16,
      "group"::binary,
      # SessionTimeout
      3600::32,
      # MemberId
      9::16,
      "member_id"::binary,
      # ProtocolType
      8::16,
      "consumer"::binary,
      # GroupProtocols array size
      1::32,
      # Basic strategy, "roundrobin" has some restrictions
      6::16,
      "assign"::binary,
      # length of metadata
      32::32,
      # v0
      0::16,
      # Topics array
      2::32,
      9::16,
      "topic_one"::binary,
      9::16,
      "topic_two"::binary,
      # UserData
      0::32
    >>

    request = %Kayrock.JoinGroup.V0.Request{
      client_id: "client_id",
      correlation_id: 42,
      group_id: "group",
      group_protocols: [
        %{
          protocol_metadata:
            <<0::16, 2::32, 9::16, "topic_one"::binary, 9::16, "topic_two"::binary, 0::32>>,
          protocol_name: "assign"
        }
      ],
      member_id: "member_id",
      protocol_type: "consumer",
      session_timeout: 3600
    }

    got = IO.iodata_to_binary(Kayrock.Request.serialize(request))

    assert got == expect, compare_binaries(got, expect)
  end
end
