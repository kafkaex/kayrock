defmodule Kayrock.IntegrationCase do
  @moduledoc """
  Testcontainer integration case template
  """
  use ExUnit.CaseTemplate

  if Code.ensure_compiled?(Testcontainers) do
    using do
      quote do
        @moduletag :integration_v2
        import Testcontainers.ExUnit

        alias Testcontainers.Container
        alias Testcontainers.KafkaContainer
      end
    end

    setup_all do
      {:ok, _pid} = Testcontainers.start_link()
      :ok
    end
  else
    defmodule TestcontainersStub do
      @moduledoc false

      def container(_name, _config, _opts) do
        :ok
      end
    end

    defmodule KafkaContainerStub do
      @moduledoc false

      def new() do
      end
    end

    using do
      quote do
        @moduletag :integration_v2
        import TestcontainersStub

        alias Kayrock.IntegrationCase.KafkaContainerStub, as: KafkaContainer
      end
    end
  end
end
