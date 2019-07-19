defmodule Kayrock.MemberAssignmentTest do
  use ExUnit.Case

  alias Kayrock.MemberAssignment

  test "deserialize empty assignments" do
    assert MemberAssignment.deserialize(<<>>) == %MemberAssignment{}
  end

  test "deserialize member assignments" do
    member_assignment = <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32>>

    assert MemberAssignment.deserialize(member_assignment) == %MemberAssignment{
             version: 0,
             partition_assignments: [
               %MemberAssignment.PartitionAssignment{topic: "topic1", partitions: [5, 3, 1]}
             ]
           }
  end

  test "deserialize member assignments with user_data" do
    member_assignment = <<0::16, 1::32, 6::16, "topic1", 3::32, 1::32, 3::32, 5::32, "user_data">>

    assert MemberAssignment.deserialize(member_assignment) == %MemberAssignment{
             version: 0,
             partition_assignments: [
               %MemberAssignment.PartitionAssignment{topic: "topic1", partitions: [5, 3, 1]}
             ],
             user_data: "user_data"
           }
  end
end
