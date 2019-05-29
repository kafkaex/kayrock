defmodule Kayrock.ClientCase do
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
