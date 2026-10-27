defmodule Kayrock.DescribeConfigsV3V4Test do
  @moduledoc """
  DescribeConfigs v3-v4, generated from the Kafka 4.4.0 JSON specs.

  Fixtures are derived by hand from `DescribeConfigsRequest.json` /
  `DescribeConfigsResponse.json` (v3 adds `IncludeDocumentation` and
  `ConfigType`/`Documentation`; v4 is the first flexible version).

  The JSON specs name fields differently from the kpro-derived v0-v2
  (`configuration_keys` vs `config_names`, `results` vs `resources`, ...).
  Booleans decode as 0/1 integers, as in every other generated version.
  """
  use ExUnit.Case, async: true

  alias Kayrock.DescribeConfigs.V3
  alias Kayrock.DescribeConfigs.V4

  test "version range" do
    assert Kayrock.DescribeConfigs.min_vsn() == 0
    assert Kayrock.DescribeConfigs.max_vsn() == 4
    assert %V3.Request{} = Kayrock.DescribeConfigs.get_request_struct(3)
    assert %V4.Request{} = Kayrock.DescribeConfigs.get_request_struct(4)
  end

  describe "v3 (non-flexible)" do
    test "request serializes" do
      request = %V3.Request{
        correlation_id: 3,
        client_id: "kayrock",
        resources: [
          %{resource_type: 2, resource_name: "topic", configuration_keys: ["cleanup.policy"]},
          %{resource_type: 4, resource_name: "1", configuration_keys: nil}
        ],
        include_synonyms: true,
        include_documentation: true
      }

      expected =
        <<32::16, 3::16, 3::32, 7::16, "kayrock">> <>
          <<2::32>> <>
          <<2, 5::16, "topic", 1::32, 14::16, "cleanup.policy">> <>
          <<4, 1::16, "1", -1::32-signed>> <>
          <<1, 1>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "response deserializes" do
      binary =
        <<3::32, 10::32>> <>
          <<1::32>> <>
          <<0::16, -1::16-signed, 2, 5::16, "topic">> <>
          <<1::32>> <>
          <<14::16, "cleanup.policy", 6::16, "delete", 0, 5, 0>> <>
          <<1::32, 14::16, "cleanup.policy", 6::16, "delete", 5>> <>
          <<2, 3::16, "doc">>

      assert V3.Response.deserialize(binary) ==
               {%V3.Response{
                  correlation_id: 3,
                  throttle_time_ms: 10,
                  results: [
                    %{
                      error_code: 0,
                      error_message: nil,
                      resource_type: 2,
                      resource_name: "topic",
                      configs: [
                        %{
                          name: "cleanup.policy",
                          value: "delete",
                          read_only: 0,
                          config_source: 5,
                          is_sensitive: 0,
                          synonyms: [%{name: "cleanup.policy", value: "delete", source: 5}],
                          config_type: 2,
                          documentation: "doc"
                        }
                      ]
                    }
                  ]
                }, <<>>}
    end
  end

  describe "v4 (flexible)" do
    test "request serializes with compact encodings and tag buffers" do
      request = %V4.Request{
        correlation_id: 4,
        client_id: "kayrock",
        resources: [
          %{
            resource_type: 2,
            resource_name: "topic",
            configuration_keys: ["cleanup.policy"],
            tagged_fields: []
          },
          %{resource_type: 4, resource_name: "1", configuration_keys: nil, tagged_fields: []}
        ],
        include_synonyms: false,
        include_documentation: true,
        tagged_fields: []
      }

      expected =
        <<32::16, 4::16, 4::32, 7::16, "kayrock", 0>> <>
          <<3>> <>
          <<2, 6, "topic", 2, 15, "cleanup.policy", 0>> <>
          <<4, 2, "1", 0, 0>> <>
          <<0, 1, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "response deserializes" do
      binary =
        <<4::32, 0>> <>
          <<0::32>> <>
          <<2>> <>
          <<0::16, 0, 2, 6, "topic">> <>
          <<2>> <>
          <<15, "cleanup.policy", 7, "delete", 0, 5, 0>> <>
          <<2, 15, "cleanup.policy", 0, 1, 0>> <>
          <<2, 0, 0>> <>
          <<0>> <>
          <<0>>

      assert V4.Response.deserialize(binary) ==
               {%V4.Response{
                  correlation_id: 4,
                  throttle_time_ms: 0,
                  results: [
                    %{
                      error_code: 0,
                      error_message: nil,
                      resource_type: 2,
                      resource_name: "topic",
                      configs: [
                        %{
                          name: "cleanup.policy",
                          value: "delete",
                          read_only: 0,
                          config_source: 5,
                          is_sensitive: 0,
                          synonyms: [
                            %{name: "cleanup.policy", value: nil, source: 1, tagged_fields: []}
                          ],
                          config_type: 2,
                          documentation: nil,
                          tagged_fields: []
                        }
                      ],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end
end
