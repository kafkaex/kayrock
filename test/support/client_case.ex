defmodule Kayrock.ClientCase do
  @moduledoc """
  Test case template for using a client

  Note there is no cleanup as the linked process will automatically be killed on
  exit
  """

  defmodule ClientCaseHelpers do
    @moduledoc """
    Helper functions imported into ClientCase tests
    """

    def unique_topic() do
      num = -:erlang.unique_integer([:positive])
      "test_topic_#{num}"
    end

    def ensure_test_topic(client, topic) do
      {:ok, _} =
        Kayrock.create_topics(
          client,
          [%{topic: topic, num_partitions: 4, replication_factor: 2}],
          1000
        )

      {:ok, topic}
    end
  end

  use ExUnit.CaseTemplate

  using do
    quote do
      @moduletag :integration

      import ClientCaseHelpers
    end
  end

  setup do
    {:ok, pid} = Kayrock.Client.start_link()

    {:ok, client: pid}
  end
end
