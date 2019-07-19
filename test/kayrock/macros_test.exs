defmodule Kayrock.MacrosTest do
  use ExUnit.Case

  use Kayrock.Macros

  # we define functions for each macro here to ensure they can be used in
  # function pattern matches

  def no_error(no_error()), do: true
  def no_error(_), do: false

  def latest_offset(latest_offset()), do: true
  def latest_offset(_), do: false

  def earliest_offset(earliest_offset()), do: true
  def earliest_offset(_), do: false

  def no_acks(no_acks()), do: true
  def no_acks(_), do: false

  def leader_ack(leader_ack()), do: true
  def leader_ack(_), do: false

  def full_isr_acks(full_isr_acks()), do: true
  def full_isr_acks(_), do: false

  test "importing macros works" do
    assert no_error(0)
    refute no_error(1)

    assert latest_offset(-1)
    refute latest_offset(0)

    assert earliest_offset(-2)
    refute earliest_offset(0)

    assert(no_acks(0))
    refute no_acks(1)

    assert leader_ack(1)
    refute leader_ack(0)

    assert full_isr_acks(-1)
    refute full_isr_acks(0)
  end
end
