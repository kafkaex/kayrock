defmodule Kayrock.LogDirsJsonVersionsTest do
  @moduledoc """
  DescribeLogDirs (key 35) v2-v5 and AlterReplicaLogDirs (key 34) v2,
  generated from the Kafka 4.4.0 JSON specs.

  DescribeLogDirs:
  * v2: first flexible version
  * v3: the response adds a top-level `ErrorCode`
  * v4: each result adds `TotalBytes` and `UsableBytes`
  * v5: each result adds `IsCordoned`
  * a null `Topics` request array means "all topics"

  AlterReplicaLogDirs:
  * v2: first flexible version

  Fixtures are derived by hand from the JSON specs.
  """
  use ExUnit.Case, async: true

  import Kayrock.Test.AdminWire

  alias Kayrock.AlterReplicaLogDirs
  alias Kayrock.DescribeLogDirs

  test "generated version ranges" do
    assert {DescribeLogDirs.min_vsn(), DescribeLogDirs.max_vsn()} == {0, 5}
    assert {AlterReplicaLogDirs.min_vsn(), AlterReplicaLogDirs.max_vsn()} == {0, 2}
  end

  describe "DescribeLogDirs" do
    for version <- 2..5 do
      test "v#{version} request serializes topics and a null (all topics) array" do
        version = unquote(version)
        module = Module.concat([DescribeLogDirs, :"V#{version}", Request])

        some =
          struct(module,
            correlation_id: version,
            client_id: "kayrock",
            topics: [%{topic: "t1", partitions: [0, 2]}]
          )

        assert serialize(some) ==
                 request_header(35, version, version, "kayrock") <>
                   carray([cstr("t1") <> carray([<<0::32>>, <<2::32>>]) <> tags()]) <> tags()

        all = struct(module, correlation_id: version, client_id: "kayrock", topics: nil)

        assert serialize(all) ==
                 request_header(35, version, version, "kayrock") <> carray(nil) <> tags()
      end
    end

    defp topics_wire do
      carray([
        cstr("t1") <>
          carray([<<0::32, 1024::64, 0::64, 0>> <> tags(), <<1::32, 0::64, 5::64, 1>> <> tags()]) <>
          tags()
      ])
    end

    defp expected_topics do
      [
        %{
          name: "t1",
          partitions: [
            %{
              partition_index: 0,
              partition_size: 1024,
              offset_lag: 0,
              is_future_key: 0,
              tagged_fields: []
            },
            %{
              partition_index: 1,
              partition_size: 0,
              offset_lag: 5,
              is_future_key: 1,
              tagged_fields: []
            }
          ],
          tagged_fields: []
        }
      ]
    end

    test "v2 response decodes" do
      wire =
        response_header(2) <>
          <<0::32>> <> carray([<<0::16>> <> cstr("/data") <> topics_wire() <> tags()]) <> tags()

      assert {response, <<>>} = DescribeLogDirs.V2.Response.deserialize(wire)

      assert response == %DescribeLogDirs.V2.Response{
               correlation_id: 2,
               throttle_time_ms: 0,
               results: [
                 %{error_code: 0, log_dir: "/data", topics: expected_topics(), tagged_fields: []}
               ],
               tagged_fields: []
             }
    end

    test "v3 response decodes the top-level error code" do
      wire = response_header(3) <> <<0::32, 31::16>> <> carray([]) <> tags()

      assert {response, <<>>} = DescribeLogDirs.V3.Response.deserialize(wire)

      assert response == %DescribeLogDirs.V3.Response{
               correlation_id: 3,
               throttle_time_ms: 0,
               error_code: 31,
               results: [],
               tagged_fields: []
             }
    end

    test "v4 response decodes total and usable bytes" do
      wire =
        response_header(4) <>
          <<0::32, 0::16>> <>
          carray([
            <<0::16>> <>
              cstr("/data") <> topics_wire() <> <<293_213_110_272::64, -1::64-signed>> <> tags()
          ]) <> tags()

      assert {response, <<>>} = DescribeLogDirs.V4.Response.deserialize(wire)

      assert response.results == [
               %{
                 error_code: 0,
                 log_dir: "/data",
                 topics: expected_topics(),
                 total_bytes: 293_213_110_272,
                 usable_bytes: -1,
                 tagged_fields: []
               }
             ]
    end

    test "v5 response decodes is_cordoned" do
      wire =
        response_header(5) <>
          <<0::32, 0::16>> <>
          carray([
            <<0::16>> <> cstr("/data") <> topics_wire() <> <<100::64, 50::64, 1>> <> tags()
          ]) <> tags()

      assert {response, <<>>} = DescribeLogDirs.V5.Response.deserialize(wire)

      assert response == %DescribeLogDirs.V5.Response{
               correlation_id: 5,
               throttle_time_ms: 0,
               error_code: 0,
               results: [
                 %{
                   error_code: 0,
                   log_dir: "/data",
                   topics: expected_topics(),
                   total_bytes: 100,
                   usable_bytes: 50,
                   is_cordoned: 1,
                   tagged_fields: []
                 }
               ],
               tagged_fields: []
             }

      assert DescribeLogDirs.deserialize(5, wire) == {response, <<>>}
    end
  end

  describe "AlterReplicaLogDirs v2" do
    test "request serializes per the JSON spec" do
      request = %AlterReplicaLogDirs.V2.Request{
        correlation_id: 2,
        client_id: "kayrock",
        dirs: [%{path: "/data2", topics: [%{name: "t1", partitions: [0, 1]}]}]
      }

      assert serialize(request) ==
               request_header(34, 2, 2, "kayrock") <>
                 carray([
                   cstr("/data2") <>
                     carray([cstr("t1") <> carray([<<0::32>>, <<1::32>>]) <> tags()]) <> tags()
                 ]) <> tags()
    end

    test "response decodes per the JSON spec" do
      wire =
        response_header(2) <>
          <<0::32>> <>
          carray([
            cstr("t1") <>
              carray([<<0::32, 0::16>> <> tags(), <<1::32, 57::16>> <> tags()]) <> tags()
          ]) <> tags()

      assert {response, <<>>} = AlterReplicaLogDirs.V2.Response.deserialize(wire)

      assert response == %AlterReplicaLogDirs.V2.Response{
               correlation_id: 2,
               throttle_time_ms: 0,
               results: [
                 %{
                   topic_name: "t1",
                   partitions: [
                     %{partition_index: 0, error_code: 0, tagged_fields: []},
                     %{partition_index: 1, error_code: 57, tagged_fields: []}
                   ],
                   tagged_fields: []
                 }
               ],
               tagged_fields: []
             }

      assert AlterReplicaLogDirs.deserialize(2, wire) == {response, <<>>}
    end
  end
end
