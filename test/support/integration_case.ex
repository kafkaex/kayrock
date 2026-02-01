defmodule Kayrock.IntegrationCase do
  @moduledoc """
  Testcontainer integration case template.

  Provides shared infrastructure for integration tests including:
  - Testcontainers setup
  - Common helper functions (via IntegrationHelpers)
  - Kafka container management

  ## Usage

      defmodule MyIntegrationTest do
        use Kayrock.IntegrationCase
        use ExUnit.Case, async: true

        container(:kafka, KafkaContainer.new(), shared: true)

        test "my test", %{kafka: kafka} do
          {:ok, client_pid} = build_client(kafka)  # From IntegrationHelpers
          topic = create_topic(client_pid, 5)       # From IntegrationHelpers
          # ... test logic
        end
      end
  """
  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration
      import Testcontainers.ExUnit
      import Kayrock.IntegrationHelpers

      alias Testcontainers.Container
      alias Testcontainers.KafkaContainer
    end
  end
end
