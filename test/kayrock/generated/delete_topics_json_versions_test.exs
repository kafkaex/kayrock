defmodule Kayrock.DeleteTopicsJsonVersionsTest do
  @moduledoc """
  DeleteTopics (key 20) v5-v6, generated from the Kafka 4.4.0 JSON specs.

  * v5: the response adds a per-topic `ErrorMessage`
  * v6: the request replaces `TopicNames` with `Topics` (`Name` nullable,
    `TopicId` uuid), so topics can be deleted by id; the response adds
    `TopicId` and makes `Name` nullable

  Fixtures are derived by hand from `DeleteTopics{Request,Response}.json`.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.DeleteTopics

  @topic_id <<0xA0A6E003C2634A1EB9A75463D296C7EC::128>>

  test "v0-v6 are generated" do
    assert DeleteTopics.min_vsn() == 0
    assert DeleteTopics.max_vsn() == 6
  end

  test "v5 request serializes topic names" do
    request = %DeleteTopics.V5.Request{
      correlation_id: 5,
      client_id: "kayrock",
      topic_names: ["a", "bb"],
      timeout_ms: 30_000
    }

    assert serialize(request) ==
             request_header(20, 5, 5, "kayrock") <>
               carray([cstr("a"), cstr("bb")]) <> <<30_000::32>> <> tags()
  end

  test "v6 request serializes topics by name and by id" do
    request = %DeleteTopics.V6.Request{
      correlation_id: 6,
      client_id: "kayrock",
      topics: [%{name: "a", topic_id: nil}, %{name: nil, topic_id: @topic_id}],
      timeout_ms: 30_000
    }

    assert serialize(request) ==
             request_header(20, 6, 6, "kayrock") <>
               carray([
                 cstr("a") <> <<0::128>> <> tags(),
                 cstr(nil) <> @topic_id <> tags()
               ]) <> <<30_000::32>> <> tags()
  end

  test "v5 response decodes the per-topic error message" do
    wire =
      response_header(5) <>
        <<0::32>> <>
        carray([
          cstr("a") <> <<0::16>> <> cstr(nil) <> tags(),
          cstr("b") <> <<3::16>> <> cstr("unknown topic") <> tags()
        ]) <> tags()

    assert {response, <<>>} = DeleteTopics.V5.Response.deserialize(wire)

    assert response == %DeleteTopics.V5.Response{
             correlation_id: 5,
             throttle_time_ms: 0,
             responses: [
               %{name: "a", error_code: 0, error_message: nil, tagged_fields: []},
               %{name: "b", error_code: 3, error_message: "unknown topic", tagged_fields: []}
             ],
             tagged_fields: []
           }
  end

  test "v6 response decodes nullable names and topic ids" do
    wire =
      response_header(6) <>
        <<0::32>> <>
        carray([
          cstr(nil) <> @topic_id <> <<0::16>> <> cstr(nil) <> tags(),
          cstr("a") <> <<0::128>> <> <<100::16>> <> cstr("UNKNOWN_TOPIC_ID") <> tags()
        ]) <> tags()

    assert {response, <<>>} = DeleteTopics.V6.Response.deserialize(wire)

    assert response == %DeleteTopics.V6.Response{
             correlation_id: 6,
             throttle_time_ms: 0,
             responses: [
               %{
                 name: nil,
                 topic_id: @topic_id,
                 error_code: 0,
                 error_message: nil,
                 tagged_fields: []
               },
               %{
                 name: "a",
                 topic_id: <<0::128>>,
                 error_code: 100,
                 error_message: "UNKNOWN_TOPIC_ID",
                 tagged_fields: []
               }
             ],
             tagged_fields: []
           }

    assert DeleteTopics.deserialize(6, wire) == {response, <<>>}
  end
end
