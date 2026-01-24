defmodule Kayrock.IntegrationCase do
  @moduledoc """
  Testcontainer integration case template
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration
      import Testcontainers.ExUnit

      alias Testcontainers.Container
      alias Testcontainers.KafkaContainer
    end
  end
end
