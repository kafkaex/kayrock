defmodule Kayrock.UserScramCredentialsTest do
  @moduledoc """
  DescribeUserScramCredentials (50) and AlterUserScramCredentials (51) v0,
  generated from the Kafka 4.4.0 JSON specs (KIP-554). v0 is flexible.

  Fixtures are derived by hand from
  `{Describe,Alter}UserScramCredentials{Request,Response}.json`. Mechanism
  1 is SCRAM-SHA-256, 2 is SCRAM-SHA-512.
  """
  use ExUnit.Case, async: true

  alias Kayrock.AlterUserScramCredentials
  alias Kayrock.DescribeUserScramCredentials

  test "API metadata" do
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_user_scram_credentials) == 50
    assert Kayrock.KafkaSchemaMetadata.api_key(:alter_user_scram_credentials) == 51
    assert Kayrock.KafkaSchemaMetadata.version_range(:describe_user_scram_credentials) == {0, 0}
    assert Kayrock.KafkaSchemaMetadata.version_range(:alter_user_scram_credentials) == {0, 0}

    assert %DescribeUserScramCredentials.V0.Request{} =
             DescribeUserScramCredentials.get_request_struct(0)

    assert %AlterUserScramCredentials.V0.Request{} =
             AlterUserScramCredentials.get_request_struct(0)
  end

  describe "DescribeUserScramCredentials v0" do
    test "request serializes (users list and null users)" do
      request = %DescribeUserScramCredentials.V0.Request{
        correlation_id: 1,
        client_id: "kayrock",
        users: [%{name: "alice", tagged_fields: []}],
        tagged_fields: []
      }

      header = <<50::16, 0::16, 1::32, 7::16, "kayrock", 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) ==
               header <> <<2, 6, "alice", 0, 0>>

      # null users = describe all users
      assert IO.iodata_to_binary(Kayrock.Request.serialize(%{request | users: nil})) ==
               header <> <<0, 0>>
    end

    test "response deserializes" do
      binary =
        <<1::32, 0>> <>
          <<0::32, 0::16, 0>> <>
          <<3>> <>
          <<6, "alice", 0::16, 0, 3, 1, 4096::32, 0, 2, 8192::32, 0, 0>> <>
          <<4, "bob", 91::16, 12, "not a user!", 1, 0>> <>
          <<0>>

      assert DescribeUserScramCredentials.V0.Response.deserialize(binary) ==
               {%DescribeUserScramCredentials.V0.Response{
                  correlation_id: 1,
                  throttle_time_ms: 0,
                  error_code: 0,
                  error_message: nil,
                  results: [
                    %{
                      user: "alice",
                      error_code: 0,
                      error_message: nil,
                      credential_infos: [
                        %{mechanism: 1, iterations: 4096, tagged_fields: []},
                        %{mechanism: 2, iterations: 8192, tagged_fields: []}
                      ],
                      tagged_fields: []
                    },
                    %{
                      user: "bob",
                      error_code: 91,
                      error_message: "not a user!",
                      credential_infos: [],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end

  describe "AlterUserScramCredentials v0" do
    test "request serializes" do
      request = %AlterUserScramCredentials.V0.Request{
        correlation_id: 2,
        client_id: "kayrock",
        deletions: [%{name: "bob", mechanism: 2, tagged_fields: []}],
        upsertions: [
          %{
            name: "alice",
            mechanism: 1,
            iterations: 4096,
            salt: <<1, 2, 3>>,
            salted_password: <<4, 5>>,
            tagged_fields: []
          }
        ],
        tagged_fields: []
      }

      expected =
        <<51::16, 0::16, 2::32, 7::16, "kayrock", 0>> <>
          <<2, 4, "bob", 2, 0>> <>
          <<2, 6, "alice", 1, 4096::32, 4, 1, 2, 3, 3, 4, 5, 0>> <>
          <<0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "response deserializes" do
      binary = <<2::32, 0, 0::32, 3, 6, "alice", 0::16, 0, 0, 4, "bob", 91::16, 1, 0, 0>>

      assert AlterUserScramCredentials.V0.Response.deserialize(binary) ==
               {%AlterUserScramCredentials.V0.Response{
                  correlation_id: 2,
                  throttle_time_ms: 0,
                  results: [
                    %{user: "alice", error_code: 0, error_message: nil, tagged_fields: []},
                    %{user: "bob", error_code: 91, error_message: "", tagged_fields: []}
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end
end
