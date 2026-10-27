defmodule Kayrock.AlterConfigsV2Test do
  @moduledoc """
  AlterConfigs v2 (first flexible version), generated from the Kafka 4.4.0
  JSON specs. Fixtures are derived by hand from `AlterConfigsRequest.json` /
  `AlterConfigsResponse.json`.

  The JSON specs name fields differently from the kpro-derived v0-v1
  (`configs`/`name`/`value` vs `config_entries`/`config_name`/`config_value`,
  `responses` vs `resources`).
  """
  use ExUnit.Case, async: true

  alias Kayrock.AlterConfigs.V2

  test "version range" do
    assert Kayrock.AlterConfigs.max_vsn() == 2
    assert %V2.Request{} = Kayrock.AlterConfigs.get_request_struct(2)
  end

  test "v2 request serializes" do
    request = %V2.Request{
      correlation_id: 2,
      client_id: "kayrock",
      resources: [
        %{
          resource_type: 2,
          resource_name: "topic",
          configs: [
            %{name: "retention.ms", value: "1000", tagged_fields: []},
            %{name: "cleanup.policy", value: nil, tagged_fields: []}
          ],
          tagged_fields: []
        }
      ],
      validate_only: true,
      tagged_fields: []
    }

    expected =
      <<33::16, 2::16, 2::32, 7::16, "kayrock", 0>> <>
        <<2, 2, 6, "topic">> <>
        <<3, 13, "retention.ms", 5, "1000", 0, 15, "cleanup.policy", 0, 0>> <>
        <<0>> <>
        <<1, 0>>

    assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
  end

  test "v2 response deserializes" do
    binary =
      <<2::32, 0>> <>
        <<5::32>> <>
        <<3>> <>
        <<0::16, 0, 2, 6, "topic", 0>> <>
        <<40::16, 6, "nope!", 2, 6, "other", 0>> <>
        <<0>>

    assert V2.Response.deserialize(binary) ==
             {%V2.Response{
                correlation_id: 2,
                throttle_time_ms: 5,
                responses: [
                  %{
                    error_code: 0,
                    error_message: nil,
                    resource_type: 2,
                    resource_name: "topic",
                    tagged_fields: []
                  },
                  %{
                    error_code: 40,
                    error_message: "nope!",
                    resource_type: 2,
                    resource_name: "other",
                    tagged_fields: []
                  }
                ],
                tagged_fields: []
              }, <<>>}
  end
end
