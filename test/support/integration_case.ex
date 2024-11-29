defmodule Kayrock.IntegrationCase do
  @moduledoc """
  Testcontainer integration case template
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration_v2
      import Testcontainers.ExUnit

      alias Testcontainers.Container
      alias Testcontainers.KafkaContainer
    end
  end

  setup_all do
    case Testcontainers.start_link() do
      {:ok, _} ->
        :ok

      {:error, {:already_started, _}} ->
        :ok
    end
  end
end
