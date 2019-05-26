defmodule Kayrock.MetadataTest do
  defmodule V0 do
    use ExUnit.Case

    alias Kayrock.Metadata.V0.Request
    alias Kayrock.Metadata.V0.Response

    import Kayrock.TestSupport

    test "correctly serializes a valid metadata request with no topics" do
      good_request = <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 0::32>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: []
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly serializes a valid metadata request with a single topic" do
      good_request = <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 1::32, 3::16, "bar"::binary>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: ["bar"]
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly serializes a valid metadata request with a multiple topics" do
      good_request =
        <<3::16, 0::16, 1::32, 3::16, "foo"::binary, 3::32, 3::16, "bar"::binary, 3::16,
          "baz"::binary, 4::16, "food"::binary>>

      request = %Request{
        correlation_id: 1,
        client_id: "foo",
        topics: ["bar", "baz", "food"]
      }

      request = IO.iodata_to_binary(Kayrock.Request.serialize(request))
      assert request == good_request, compare_binaries(request, good_request)
    end

    test "correctly deserializes a valid response" do
      response =
        <<0::32, 1::32, 0::32, 3::16, "foo"::binary, 9092::32, 1::32, 0::16, 3::16, "bar"::binary,
          1::32, 0::16, 0::32, 0::32, 0::32, 1::32, 0::32>>

      expect = %Response{
        correlation_id: 0,
        brokers: [
          %{node_id: 0, host: "foo", port: 9092}
        ],
        topic_metadata: [
          %{
            error_code: 0,
            topic: "bar",
            partition_metadata: [
              %{
                error_code: 0,
                partition: 0,
                replicas: [],
                isr: [0],
                leader: 0
              }
            ]
          }
        ]
      }

      {got, <<>>} = Kayrock.Metadata.deserialize(0, response)
      assert got == expect
    end
  end

  defmodule V1 do
    use ExUnit.Case

    alias Kayrock.Metadata.V0.Response

    test "correctly handles an empty topic list" do
      data =
        <<0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 0, 0, 9, 49, 48, 46, 48, 46, 49, 46, 50, 49, 0, 0, 35,
          132, 255, 255, 0, 0, 0, 0, 0, 0, 0, 0>>

      {rsp, _} = Response.deserialize(data)

      assert rsp.topic_metadata == []
    end
  end
end
