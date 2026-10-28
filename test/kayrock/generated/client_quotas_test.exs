defmodule Kayrock.ClientQuotasTest do
  @moduledoc """
  DescribeClientQuotas (48) and AlterClientQuotas (49) v0-v1, generated from
  the Kafka 4.4.0 JSON specs (KIP-546). v1 is the first flexible version.

  Fixtures are derived by hand from `{Describe,Alter}ClientQuotas{Request,Response}.json`.
  Quota values are `float64` (big-endian IEEE 754).
  """
  use ExUnit.Case, async: true

  test "API metadata" do
    assert Kayrock.KafkaSchemaMetadata.api_key(:describe_client_quotas) == 48
    assert Kayrock.KafkaSchemaMetadata.api_key(:alter_client_quotas) == 49
    assert Kayrock.KafkaSchemaMetadata.version_range(:describe_client_quotas) == {0, 1}
    assert Kayrock.KafkaSchemaMetadata.version_range(:alter_client_quotas) == {0, 1}

    for api <- [Kayrock.DescribeClientQuotas, Kayrock.AlterClientQuotas], vsn <- 0..1 do
      assert %{__struct__: struct} = api.get_request_struct(vsn)
      assert struct.api_vsn() == vsn
    end
  end

  describe "DescribeClientQuotas" do
    test "v0 request serializes" do
      request = %Kayrock.DescribeClientQuotas.V0.Request{
        correlation_id: 1,
        client_id: "kayrock",
        components: [
          %{entity_type: "user", match_type: 0, match: "alice"},
          %{entity_type: "client-id", match_type: 2, match: nil}
        ],
        strict: true
      }

      expected =
        <<48::16, 0::16, 1::32, 7::16, "kayrock">> <>
          <<2::32>> <>
          <<4::16, "user", 0, 5::16, "alice">> <>
          <<9::16, "client-id", 2, -1::16-signed>> <>
          <<1>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v1 request serializes" do
      request = %Kayrock.DescribeClientQuotas.V1.Request{
        correlation_id: 1,
        client_id: "kayrock",
        components: [%{entity_type: "user", match_type: 1, match: nil, tagged_fields: []}],
        strict: false,
        tagged_fields: []
      }

      expected =
        <<48::16, 1::16, 1::32, 7::16, "kayrock", 0>> <>
          <<2, 5, "user", 1, 0, 0>> <> <<0, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v0 response deserializes" do
      binary =
        <<1::32>> <>
          <<0::32, 0::16, -1::16-signed>> <>
          <<1::32>> <>
          <<1::32, 4::16, "user", 5::16, "alice">> <>
          <<2::32>> <>
          <<18::16, "producer_byte_rate", 1024.0::64-float>> <>
          <<18::16, "consumer_byte_rate", 0.5::64-float>>

      assert Kayrock.DescribeClientQuotas.V0.Response.deserialize(binary) ==
               {%Kayrock.DescribeClientQuotas.V0.Response{
                  correlation_id: 1,
                  throttle_time_ms: 0,
                  error_code: 0,
                  error_message: nil,
                  entries: [
                    %{
                      entity: [%{entity_type: "user", entity_name: "alice"}],
                      values: [
                        %{key: "producer_byte_rate", value: 1024.0},
                        %{key: "consumer_byte_rate", value: 0.5}
                      ]
                    }
                  ]
                }, <<>>}
    end

    test "v1 response deserializes (default entity, null entries)" do
      binary =
        <<1::32, 0>> <>
          <<0::32, 0::16, 0>> <>
          <<2>> <>
          <<2, 5, "user", 0, 0>> <>
          <<2, 19, "producer_byte_rate", 2048.0::64-float, 0>> <>
          <<0>> <>
          <<0>>

      assert Kayrock.DescribeClientQuotas.V1.Response.deserialize(binary) ==
               {%Kayrock.DescribeClientQuotas.V1.Response{
                  correlation_id: 1,
                  throttle_time_ms: 0,
                  error_code: 0,
                  error_message: nil,
                  entries: [
                    %{
                      entity: [%{entity_type: "user", entity_name: nil, tagged_fields: []}],
                      values: [%{key: "producer_byte_rate", value: 2048.0, tagged_fields: []}],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}

      # Entries is nullable: an error response carries null entries
      error = <<1::32, 0, 0::32, 42::16, 4, "bad", 0, 0>>

      assert {%{error_code: 42, error_message: "bad", entries: nil}, <<>>} =
               Kayrock.DescribeClientQuotas.V1.Response.deserialize(error)
    end
  end

  describe "AlterClientQuotas" do
    test "v0 request serializes" do
      request = %Kayrock.AlterClientQuotas.V0.Request{
        correlation_id: 2,
        client_id: "kayrock",
        entries: [
          %{
            entity: [%{entity_type: "user", entity_name: nil}],
            ops: [
              %{key: "producer_byte_rate", value: 1024.0, remove: false},
              %{key: "consumer_byte_rate", value: 0.0, remove: true}
            ]
          }
        ],
        validate_only: false
      }

      expected =
        <<49::16, 0::16, 2::32, 7::16, "kayrock">> <>
          <<1::32>> <>
          <<1::32, 4::16, "user", -1::16-signed>> <>
          <<2::32>> <>
          <<18::16, "producer_byte_rate", 1024.0::64-float, 0>> <>
          <<18::16, "consumer_byte_rate", 0.0::64-float, 1>> <>
          <<0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v1 request serializes" do
      request = %Kayrock.AlterClientQuotas.V1.Request{
        correlation_id: 2,
        client_id: "kayrock",
        entries: [
          %{
            entity: [%{entity_type: "client-id", entity_name: "app", tagged_fields: []}],
            ops: [%{key: "request_percentage", value: 12.5, remove: false, tagged_fields: []}],
            tagged_fields: []
          }
        ],
        validate_only: true,
        tagged_fields: []
      }

      expected =
        <<49::16, 1::16, 2::32, 7::16, "kayrock", 0>> <>
          <<2>> <>
          <<2, 10, "client-id", 4, "app", 0>> <>
          <<2, 19, "request_percentage", 12.5::64-float, 0, 0>> <>
          <<0>> <>
          <<1, 0>>

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expected
    end

    test "v0 response deserializes" do
      binary =
        <<2::32, 0::32, 1::32>> <>
          <<0::16, -1::16-signed, 1::32, 4::16, "user", 5::16, "alice">>

      assert Kayrock.AlterClientQuotas.V0.Response.deserialize(binary) ==
               {%Kayrock.AlterClientQuotas.V0.Response{
                  correlation_id: 2,
                  throttle_time_ms: 0,
                  entries: [
                    %{
                      error_code: 0,
                      error_message: nil,
                      entity: [%{entity_type: "user", entity_name: "alice"}]
                    }
                  ]
                }, <<>>}
    end

    test "v1 response deserializes" do
      binary =
        <<2::32, 0, 3::32>> <>
          <<2, 42::16, 4, "bad", 2, 10, "client-id", 0, 0, 0>> <>
          <<0>>

      assert Kayrock.AlterClientQuotas.V1.Response.deserialize(binary) ==
               {%Kayrock.AlterClientQuotas.V1.Response{
                  correlation_id: 2,
                  throttle_time_ms: 3,
                  entries: [
                    %{
                      error_code: 42,
                      error_message: "bad",
                      entity: [%{entity_type: "client-id", entity_name: nil, tagged_fields: []}],
                      tagged_fields: []
                    }
                  ],
                  tagged_fields: []
                }, <<>>}
    end
  end
end
