defmodule Kayrock.CreateTopicsJsonVersionsTest do
  @moduledoc """
  CreateTopics (key 19) v6-v7, generated from the Kafka 4.4.0 JSON specs.

  * v6: same schema as v5 (may return THROTTLING_QUOTA_EXCEEDED, KIP-599)
  * v7: the response adds the created topic's `TopicId` (uuid)

  Both carry the named tagged field `TopicConfigErrorCode` (tag 0) per topic.
  Fixtures are derived by hand from `CreateTopics{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.CreateTopics

  @topic_id <<0x0123456789ABCDEF0123456789ABCDEF::128>>

  test "v0-v7 are generated" do
    assert CreateTopics.min_vsn() == 0
    assert CreateTopics.max_vsn() == 7
  end

  defp request(version) do
    struct(Module.concat([CreateTopics, :"V#{version}", Request]),
      correlation_id: version,
      client_id: "kayrock",
      topics: [
        %{
          name: "t1",
          num_partitions: -1,
          replication_factor: -1,
          assignments: [%{partition_index: 0, broker_ids: [1, 2]}],
          configs: [%{name: "cleanup.policy", value: "compact"}, %{name: "x", value: nil}]
        }
      ],
      timeout_ms: 10_000,
      validate_only: true
    )
  end

  defp request_body do
    carray([
      cstr("t1") <>
        <<-1::32-signed, -1::16-signed>> <>
        carray([<<0::32>> <> carray([<<1::32>>, <<2::32>>]) <> tags()]) <>
        carray([
          cstr("cleanup.policy") <> cstr("compact") <> tags(),
          cstr("x") <> cstr(nil) <> tags()
        ]) <>
        tags()
    ]) <> <<10_000::32, 1>> <> tags()
  end

  for version <- [6, 7] do
    test "v#{version} request serializes per the JSON spec" do
      version = unquote(version)

      assert serialize(request(version)) ==
               request_header(19, version, version, "kayrock") <> request_body()
    end
  end

  defp config_entry do
    cstr("cleanup.policy") <> cstr("delete") <> <<0, 5, 0>> <> tags()
  end

  test "v6 response decodes, including the TopicConfigErrorCode tagged field" do
    wire =
      response_header(6) <>
        <<0::32>> <>
        carray([
          cstr("t1") <>
            <<0::16>> <>
            cstr(nil) <>
            <<3::32, 1::16>> <> carray([config_entry()]) <> uvarint(1) <> tag(0, <<29::16>>),
          cstr("t2") <>
            <<36::16>> <>
            cstr("exists") <> <<-1::32-signed, -1::16-signed>> <> carray(nil) <> tags()
        ]) <> tags()

    assert {response, <<>>} = CreateTopics.V6.Response.deserialize(wire)

    assert response == %CreateTopics.V6.Response{
             correlation_id: 6,
             throttle_time_ms: 0,
             topics: [
               %{
                 name: "t1",
                 error_code: 0,
                 error_message: nil,
                 num_partitions: 3,
                 replication_factor: 1,
                 configs: [
                   %{
                     name: "cleanup.policy",
                     value: "delete",
                     read_only: 0,
                     config_source: 5,
                     is_sensitive: 0,
                     tagged_fields: []
                   }
                 ],
                 topic_config_error_code: 29,
                 tagged_fields: []
               },
               %{
                 name: "t2",
                 error_code: 36,
                 error_message: "exists",
                 num_partitions: -1,
                 replication_factor: -1,
                 configs: nil,
                 topic_config_error_code: 0,
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }
  end

  test "v7 response decodes the topic id" do
    wire =
      response_header(7) <>
        <<5::32>> <>
        carray([
          cstr("t1") <>
            @topic_id <>
            <<0::16>> <> cstr(nil) <> <<3::32, 1::16>> <> carray([config_entry()]) <> tags()
        ]) <> tags()

    assert {response, <<>>} = CreateTopics.V7.Response.deserialize(wire)
    assert response.correlation_id == 7
    assert response.throttle_time_ms == 5

    assert [
             %{
               name: "t1",
               topic_id: @topic_id,
               error_code: 0,
               error_message: nil,
               num_partitions: 3,
               replication_factor: 1,
               topic_config_error_code: 0,
               configs: [%{name: "cleanup.policy", value: "delete", config_source: 5}]
             }
           ] = response.topics

    assert CreateTopics.deserialize(7, wire) == {response, <<>>}
  end
end
