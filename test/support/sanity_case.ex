defmodule Kayrock.SanityCase do
  @moduledoc """
  ExUnit case template for sanity integration tests.

  Tags tests with :sanity (excluded by default).
  Imports IntegrationHelpers and TestSupport.
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :sanity
      import Testcontainers.ExUnit
      import Kayrock.IntegrationHelpers
      import Kayrock.TestSupport

      alias Testcontainers.Container
      alias Testcontainers.KafkaContainer
    end
  end
end
