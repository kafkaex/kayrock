defmodule Kayrock.MemberAssignmentTest do
  use ExUnit.Case

  alias Kayrock.MemberAssignment

  describe "deserialize/1" do
    test "deserialize empty assignments" do
      assert MemberAssignment.deserialize(<<>>) == {%MemberAssignment{}, <<>>}
    end

    # This matches the response from an error response to SyncGroup, for example
    test "deserialize no assignments" do
      member_assignment = <<0, 0, 0, 0>>
      {got, ""} = MemberAssignment.deserialize(member_assignment)
      assert got == %MemberAssignment{version: 0}
    end

    test "deserialize member assignments" do
      member_assignment = <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32>>
      member_assignment = <<byte_size(member_assignment)::32-signed, member_assignment::bytes>>

      {got, ""} = MemberAssignment.deserialize(member_assignment)

      assert got == %MemberAssignment{
               version: 0,
               partition_assignments: [
                 %MemberAssignment.PartitionAssignment{topic: "topic1", partitions: [5, 3, 1]}
               ]
             }
    end

    test "deserialize member assignments with user_data" do
      member_assignment =
        <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32, 9::32-signed, "user_data">>

      member_assignment = <<byte_size(member_assignment)::32-signed, member_assignment::bytes>>

      {got, ""} = MemberAssignment.deserialize(member_assignment)

      assert got == %MemberAssignment{
               version: 0,
               partition_assignments: [
                 %MemberAssignment.PartitionAssignment{topic: "topic1", partitions: [5, 3, 1]}
               ],
               user_data: "user_data"
             }
    end

    test "deserialize member assignments in partition rebalanced time - no membership assigment" do
      member_assignment = <<0, 0, 0, 0, 0, 53>>

      {got, <<0, 53>>} = MemberAssignment.deserialize(member_assignment)

      assert got == %Kayrock.MemberAssignment{
               partition_assignments: [],
               user_data: "",
               version: 0
             }
    end

    test "full sync_group response" do
      data =
        <<0, 0, 0, 4, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 2, 0, 3, 102, 111, 111, 0, 0, 0, 1, 0, 0,
          0, 1, 0, 3, 98, 97, 114, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0>>

      {got, ""} = Kayrock.SyncGroup.V0.Response.deserialize(data)

      # The response assignment is now automatically deserialized to a MemberAssignment struct
      assert %MemberAssignment{} = got.assignment
      deserialized_assignment = got.assignment

      assert got.correlation_id == 4
      assert got.error_code == 0

      # Check partitions are in the right order
      assert length(deserialized_assignment.partition_assignments) == 2

      foo_assignment =
        Enum.find(deserialized_assignment.partition_assignments, fn pa -> pa.topic == "foo" end)

      bar_assignment =
        Enum.find(deserialized_assignment.partition_assignments, fn pa -> pa.topic == "bar" end)

      assert foo_assignment == %MemberAssignment.PartitionAssignment{
               topic: "foo",
               partitions: [1]
             }

      assert bar_assignment == %MemberAssignment.PartitionAssignment{
               topic: "bar",
               partitions: [2]
             }

      assert deserialized_assignment.version == 0
      assert deserialized_assignment.user_data == ""
    end
  end

  describe "round-trip serialize/deserialize" do
    test "round-trip with non-empty user_data preserves content" do
      original = %MemberAssignment{
        version: 0,
        partition_assignments: [
          %MemberAssignment.PartitionAssignment{topic: "orders", partitions: [0, 1, 2]}
        ],
        user_data: "sticky-assignor-state"
      }

      serialized = IO.iodata_to_binary(MemberAssignment.serialize(original))
      wire = <<byte_size(serialized)::32-signed, serialized::binary>>
      {deserialized, <<>>} = MemberAssignment.deserialize(wire)

      assert deserialized.version == original.version
      assert deserialized.user_data == "sticky-assignor-state"
      assert length(deserialized.partition_assignments) == 1
      [pa] = deserialized.partition_assignments
      assert pa.topic == "orders"
      assert Enum.sort(pa.partitions) == [0, 1, 2]
    end

    test "round-trip with empty user_data" do
      original = %MemberAssignment{
        version: 0,
        partition_assignments: [
          %MemberAssignment.PartitionAssignment{topic: "events", partitions: [0]}
        ],
        user_data: ""
      }

      serialized = IO.iodata_to_binary(MemberAssignment.serialize(original))
      wire = <<byte_size(serialized)::32-signed, serialized::binary>>
      {deserialized, <<>>} = MemberAssignment.deserialize(wire)

      assert deserialized.user_data == ""
      assert deserialized.version == 0
    end

    test "round-trip with binary user_data" do
      original = %MemberAssignment{
        version: 1,
        partition_assignments: [],
        user_data: <<0xFF, 0xFE, 0x00, 0x01>>
      }

      serialized = IO.iodata_to_binary(MemberAssignment.serialize(original))
      wire = <<byte_size(serialized)::32-signed, serialized::binary>>
      {deserialized, <<>>} = MemberAssignment.deserialize(wire)

      assert deserialized.user_data == <<0xFF, 0xFE, 0x00, 0x01>>
      assert deserialized.version == 1
    end

    test "round-trip with multiple topics preserves all data" do
      original = %MemberAssignment{
        version: 0,
        partition_assignments: [
          %MemberAssignment.PartitionAssignment{topic: "foo", partitions: [0, 3]},
          %MemberAssignment.PartitionAssignment{topic: "bar", partitions: [1]}
        ],
        user_data: "test"
      }

      serialized = IO.iodata_to_binary(MemberAssignment.serialize(original))
      wire = <<byte_size(serialized)::32-signed, serialized::binary>>
      {deserialized, <<>>} = MemberAssignment.deserialize(wire)

      assert deserialized.version == 0
      assert deserialized.user_data == "test"
      assert length(deserialized.partition_assignments) == 2

      topics =
        deserialized.partition_assignments
        |> Enum.map(& &1.topic)
        |> Enum.sort()

      assert topics == ["bar", "foo"]

      foo = Enum.find(deserialized.partition_assignments, &(&1.topic == "foo"))
      assert Enum.sort(foo.partitions) == [0, 3]

      bar = Enum.find(deserialized.partition_assignments, &(&1.topic == "bar"))
      assert bar.partitions == [1]
    end
  end

  describe "deserialize_content/1" do
    test "parses raw inner bytes without length prefix" do
      inner =
        <<0::16, 1::32, 5::16, "topic", 2::32, 0::32, 1::32, 0::32, "">>

      result = MemberAssignment.deserialize_content(inner)

      assert %MemberAssignment{} = result
      assert result.version == 0
      assert length(result.partition_assignments) == 1
      [pa] = result.partition_assignments
      assert pa.topic == "topic"
      assert Enum.sort(pa.partitions) == [0, 1]
    end

    test "handles empty binary" do
      assert %MemberAssignment{} = MemberAssignment.deserialize_content(<<>>)
    end
  end

  describe "serialize/1" do
    test "serialize sync_group request" do
      expect =
        <<0, 14, 0, 0, 0, 0, 0, 99, 0, 3, 102, 111, 111, 0, 5, 103, 114, 111, 117, 112, 0, 0, 0,
          0, 0, 5, 109, 97, 114, 103, 101, 0, 0, 0, 1, 0, 6, 116, 104, 101, 108, 109, 97, 0, 0, 0,
          36, 0, 0, 0, 0, 0, 1, 0, 12, 99, 104, 97, 105, 110, 115, 109, 111, 107, 105, 110, 103,
          0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0>>

      request = %Kayrock.SyncGroup.V0.Request{
        client_id: "foo",
        correlation_id: 99,
        generation_id: 0,
        assignments: [
          %{
            assignment:
              IO.iodata_to_binary(
                Kayrock.MemberAssignment.serialize(%Kayrock.MemberAssignment{
                  partition_assignments: [
                    %Kayrock.MemberAssignment.PartitionAssignment{
                      partitions: [0, 3],
                      topic: "chainsmoking"
                    }
                  ],
                  user_data: "",
                  version: 0
                })
              ),
            member_id: "thelma"
          }
        ],
        group_id: "group",
        member_id: "marge"
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expect
    end

    test "serialize sync_group request - member assignment preserialized" do
      expect =
        <<0, 14, 0, 0, 0, 0, 0, 99, 0, 3, 102, 111, 111, 0, 5, 103, 114, 111, 117, 112, 0, 0, 0,
          0, 0, 5, 109, 97, 114, 103, 101, 0, 0, 0, 1, 0, 6, 116, 104, 101, 108, 109, 97, 0, 0, 0,
          36, 0, 0, 0, 0, 0, 1, 0, 12, 99, 104, 97, 105, 110, 115, 109, 111, 107, 105, 110, 103,
          0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0>>

      request = %Kayrock.SyncGroup.V0.Request{
        client_id: "foo",
        correlation_id: 99,
        generation_id: 0,
        assignments: [
          %{
            assignment:
              <<0, 0, 0, 0, 0, 1, 0, 12, 99, 104, 97, 105, 110, 115, 109, 111, 107, 105, 110, 103,
                0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0>>,
            member_id: "thelma"
          }
        ],
        group_id: "group",
        member_id: "marge"
      }

      assert IO.iodata_to_binary(Kayrock.Request.serialize(request)) == expect
    end
  end
end
