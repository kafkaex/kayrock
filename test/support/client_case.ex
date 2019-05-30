defmodule Kayrock.ClientCase do
  @moduledoc """
  Test case template for using a client

  Note there is no cleanup as the linked process will automatically be killed on
  exit
  """

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration
    end
  end

  setup do
    {:ok, pid} = Kayrock.Client.start_link()

    {:ok, client: pid}
  end
end
