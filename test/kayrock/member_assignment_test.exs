defmodule Kayrock.MemberAssignmentTest do
  use ExUnit.Case

  alias Kayrock.MemberAssignment

  test "deserialize empty assignments" do
    assert MemberAssignment.deserialize(<<>>) == {%MemberAssignment{}, <<>>}
  end

  # This matches the response from an error response to SyncGroup, for example
  test "deserialize no assignements" do
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
    member_assignment = <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32, "user_data">>
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

  test "full sync_group response" do
    data =
      <<0, 0, 0, 4, 0, 0, 0, 0, 0, 36, 0, 0, 0, 0, 0, 2, 0, 3, 102, 111, 111, 0, 0, 0, 1, 0, 0, 0,
        1, 0, 3, 98, 97, 114, 0, 0, 0, 1, 0, 0, 0, 2, 0, 0, 0, 0>>

    {got, ""} = Kayrock.SyncGroup.V0.Response.deserialize(data)

    assert got == %Kayrock.SyncGroup.V0.Response{
             correlation_id: 4,
             error_code: 0,
             member_assignment: %MemberAssignment{
               partition_assignments: [
                 %MemberAssignment.PartitionAssignment{
                   topic: "bar",
                   partitions: [2]
                 },
                 %MemberAssignment.PartitionAssignment{
                   topic: "foo",
                   partitions: [1]
                 }
               ],
               version: 0,
               user_data: <<0, 0, 0, 0>>
             }
           }
  end

  test "serialize sync_group request" do
    expect =
      <<0, 14, 0, 0, 0, 0, 0, 99, 0, 3, 102, 111, 111, 0, 5, 103, 114, 111, 117, 112, 0, 0, 0, 0,
        0, 5, 109, 97, 114, 103, 101, 0, 0, 0, 1, 0, 6, 116, 104, 101, 108, 109, 97, 0, 0, 0, 36,
        0, 0, 0, 0, 0, 1, 0, 12, 99, 104, 97, 105, 110, 115, 109, 111, 107, 105, 110, 103, 0, 0,
        0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0>>

    request = %Kayrock.SyncGroup.V0.Request{
      client_id: "foo",
      correlation_id: 99,
      generation_id: 0,
      group_assignment: [
        %{
          member_assignment: %Kayrock.MemberAssignment{
            partition_assignments: [
              %Kayrock.MemberAssignment.PartitionAssignment{
                partitions: [0, 3],
                topic: "chainsmoking"
              }
            ],
            user_data: "",
            version: 0
          },
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
      <<0, 14, 0, 0, 0, 0, 0, 99, 0, 3, 102, 111, 111, 0, 5, 103, 114, 111, 117, 112, 0, 0, 0, 0,
        0, 5, 109, 97, 114, 103, 101, 0, 0, 0, 1, 0, 6, 116, 104, 101, 108, 109, 97, 0, 0, 0, 36,
        0, 0, 0, 0, 0, 1, 0, 12, 99, 104, 97, 105, 110, 115, 109, 111, 107, 105, 110, 103, 0, 0,
        0, 2, 0, 0, 0, 0, 0, 0, 0, 3, 0, 0, 0, 0>>

    request = %Kayrock.SyncGroup.V0.Request{
      client_id: "foo",
      correlation_id: 99,
      generation_id: 0,
      group_assignment: [
        %{
          member_assignment:
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
