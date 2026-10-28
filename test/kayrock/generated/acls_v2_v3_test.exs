defmodule Kayrock.AclsV2V3Test do
  @moduledoc """
  DescribeAcls / CreateAcls / DeleteAcls v2-v3, generated from the Kafka 4.4.0
  JSON specs. v2 is the first flexible version; v3 adds the `USER_GROUP`
  resource type (KIP-1205) and is wire-identical to v2.

  Fixtures are derived by hand from `{Describe,Create,Delete}Acls{Request,Response}.json`.

  The JSON specs name fields differently from the kpro-derived v0-v1
  (`resource_type_filter` vs `resource_type`, `pattern_type` vs
  `resource_pattern_type`, `results` vs `creation_responses`,
  `filter_results` vs `filter_responses`).
  """
  use ExUnit.Case, async: true

  @versions 2..3

  test "version ranges" do
    for mod <- [Kayrock.DescribeAcls, Kayrock.CreateAcls, Kayrock.DeleteAcls] do
      assert mod.min_vsn() == 0
      assert mod.max_vsn() == 3
    end
  end

  defp mod(api, vsn, kind), do: Module.concat([Kayrock, api, "V#{vsn}", kind])

  defp header(api_key, vsn, correlation_id),
    do: <<api_key::16, vsn::16, correlation_id::32, 7::16, "kayrock", 0>>

  describe "DescribeAcls" do
    test "request serializes" do
      for vsn <- @versions do
        request =
          struct(mod(DescribeAcls, vsn, Request),
            correlation_id: vsn,
            client_id: "kayrock",
            resource_type_filter: 2,
            resource_name_filter: "topic",
            pattern_type_filter: 3,
            principal_filter: nil,
            host_filter: "*",
            operation: 1,
            permission_type: 3,
            tagged_fields: []
          )

        expected = header(29, vsn, vsn) <> <<2, 6, "topic", 3, 0, 2, "*", 1, 3, 0>>
        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      end
    end

    test "response deserializes" do
      for vsn <- @versions do
        binary =
          <<vsn::32, 0>> <>
            <<7::32, 0::16, 0>> <>
            <<2>> <>
            <<2, 6, "topic", 3>> <>
            <<2, 11, "User:alice", 2, "*", 3, 3, 0>> <>
            <<0>> <>
            <<0>>

        assert mod(DescribeAcls, vsn, Response).deserialize(binary) ==
                 {struct(mod(DescribeAcls, vsn, Response),
                    correlation_id: vsn,
                    throttle_time_ms: 7,
                    error_code: 0,
                    error_message: nil,
                    resources: [
                      %{
                        resource_type: 2,
                        resource_name: "topic",
                        pattern_type: 3,
                        acls: [
                          %{
                            principal: "User:alice",
                            host: "*",
                            operation: 3,
                            permission_type: 3,
                            tagged_fields: []
                          }
                        ],
                        tagged_fields: []
                      }
                    ],
                    tagged_fields: []
                  ), <<>>}
      end
    end

    test "security-disabled error response deserializes" do
      binary = <<1::32, 0, 0::32, 55::16, 6, "nope!", 1, 0>>

      assert {%{error_code: 55, error_message: "nope!", resources: []}, <<>>} =
               mod(DescribeAcls, 3, Response).deserialize(binary)
    end
  end

  describe "CreateAcls" do
    test "request serializes" do
      for vsn <- @versions do
        request =
          struct(mod(CreateAcls, vsn, Request),
            correlation_id: vsn,
            client_id: "kayrock",
            creations: [
              %{
                resource_type: 2,
                resource_name: "topic",
                resource_pattern_type: 3,
                principal: "User:alice",
                host: "*",
                operation: 3,
                permission_type: 3,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          )

        expected =
          header(30, vsn, vsn) <>
            <<2, 2, 6, "topic", 3, 11, "User:alice", 2, "*", 3, 3, 0>> <> <<0>>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      end
    end

    test "response deserializes" do
      for vsn <- @versions do
        binary = <<vsn::32, 0, 0::32, 3, 0::16, 0, 0, 54::16, 4, "bad", 0, 0>>

        assert mod(CreateAcls, vsn, Response).deserialize(binary) ==
                 {struct(mod(CreateAcls, vsn, Response),
                    correlation_id: vsn,
                    throttle_time_ms: 0,
                    results: [
                      %{error_code: 0, error_message: nil, tagged_fields: []},
                      %{error_code: 54, error_message: "bad", tagged_fields: []}
                    ],
                    tagged_fields: []
                  ), <<>>}
      end
    end
  end

  describe "DeleteAcls" do
    test "request serializes" do
      for vsn <- @versions do
        request =
          struct(mod(DeleteAcls, vsn, Request),
            correlation_id: vsn,
            client_id: "kayrock",
            filters: [
              %{
                resource_type_filter: 1,
                resource_name_filter: nil,
                pattern_type_filter: 1,
                principal_filter: "User:alice",
                host_filter: nil,
                operation: 1,
                permission_type: 1,
                tagged_fields: []
              }
            ],
            tagged_fields: []
          )

        expected =
          header(31, vsn, vsn) <> <<2, 1, 0, 1, 11, "User:alice", 0, 1, 1, 0>> <> <<0>>

        assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
      end
    end

    test "response deserializes" do
      for vsn <- @versions do
        binary =
          <<vsn::32, 0>> <>
            <<0::32>> <>
            <<2>> <>
            <<0::16, 0>> <>
            <<2>> <>
            <<0::16, 0, 2, 6, "topic", 3, 11, "User:alice", 2, "*", 3, 3, 0>> <>
            <<0>> <>
            <<0>>

        assert mod(DeleteAcls, vsn, Response).deserialize(binary) ==
                 {struct(mod(DeleteAcls, vsn, Response),
                    correlation_id: vsn,
                    throttle_time_ms: 0,
                    filter_results: [
                      %{
                        error_code: 0,
                        error_message: nil,
                        matching_acls: [
                          %{
                            error_code: 0,
                            error_message: nil,
                            resource_type: 2,
                            resource_name: "topic",
                            pattern_type: 3,
                            principal: "User:alice",
                            host: "*",
                            operation: 3,
                            permission_type: 3,
                            tagged_fields: []
                          }
                        ],
                        tagged_fields: []
                      }
                    ],
                    tagged_fields: []
                  ), <<>>}
      end
    end
  end
end
