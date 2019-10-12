defmodule KayrockTest do
  use ExUnit.Case
  doctest Kayrock

  test "generates a helpeful error message on serialization failure" do
    assert_raise(Kayrock.InvalidRequestError, ~r/.*Kayrock.Fetch.V0.*/, fn ->
      Kayrock.Request.serialize(%Kayrock.Fetch.V0.Request{})
    end)
  end
end
