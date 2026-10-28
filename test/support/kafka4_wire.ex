defmodule Kayrock.Kafka4Wire do
  @moduledoc """
  Raw-wire helpers for `:kafka4` tests of the data-plane APIs (Produce,
  Fetch, ListOffsets, DeleteRecords, OffsetForLeaderEpoch).

    * `call/2` sends one request on its own `Kayrock.BrokerConnection` and
      returns the decoded response, asserting the response decodes with no
      leftover bytes (`Kayrock.client_call/3` drops the remainder).
    * `topic_id/2` looks a topic's UUID up with Metadata v12. Kayrock does
      not generate a Metadata version that carries topic IDs (v10+), so a
      test-only Metadata v12 is compiled straight from the vendored JSON spec
      (`Kayrock.Test.GeneratedCode`, nothing is written to `lib/generated`).
  """

  import ExUnit.Assertions

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.JsonSpec
  alias Kayrock.Test.GeneratedCode

  @namespace Kayrock.Kafka4Wire.Gen
  @metadata_vsn 12

  defmodule MetadataSchema do
    @moduledoc false
    # A kpro_schema-shaped source serving only Metadata v12 from the JSON specs.
    alias Kayrock.Generate.ApiTable
    alias Kayrock.Generate.JsonSpec

    defp spec, do: JsonSpec.load!(ApiTable.spec_dir())

    def vsn_range(:metadata), do: {12, 12}
    def min_flexible_vsn(api), do: JsonSpec.min_flexible_version(spec(), api)
    def api_key(api), do: JsonSpec.api_key(spec(), api)
    def req(api, vsn), do: JsonSpec.request_schema(spec(), api, vsn)
    def rsp(api, vsn), do: JsonSpec.response_schema(spec(), api, vsn)
    def request_header_vsn(api, vsn), do: JsonSpec.request_header_version(spec(), api, vsn)
    def response_header_vsn(api, vsn), do: JsonSpec.response_header_version(spec(), api, vsn)
  end

  @doc """
  Sends `request` (a struct whose module has `serialize/1` and
  `response_deserializer/0`) to the broker on a fresh connection and returns
  the response. Fails the test unless the whole response is consumed.
  """
  @spec call(Testcontainers.Container.t(), struct()) :: map()
  def call(kafka, %module{} = request) do
    request = %{request | correlation_id: 4242, client_id: "kayrock-kafka4"}
    {:ok, conn} = Kayrock.Kafka4Helpers.connect_broker(kafka)

    try do
      :ok = Kayrock.BrokerConnection.send(conn, module.serialize(request))
      {:ok, data} = Kayrock.BrokerConnection.recv(conn, 10_000)
      {response, rest} = module.response_deserializer().(data)

      assert rest == <<>>,
             "#{inspect(module)}: #{byte_size(rest)} leftover bytes after decoding"

      assert response.correlation_id == 4242
      response
    after
      Kayrock.BrokerConnection.stop(conn)
    end
  end

  @doc """
  Sends a single-partition Produce request with `call/2`, retrying while the
  partition reports NOT_LEADER_OR_FOLLOWER (6) or UNKNOWN_TOPIC_OR_PARTITION
  (3): right after CreateTopics, Metadata can report a leader before that
  leader accepts writes. Nothing is appended on those errors, so retrying
  does not duplicate records. Returns the last response.
  """
  @spec produce(Testcontainers.Container.t(), struct(), non_neg_integer()) :: map()
  def produce(kafka, request, retries \\ 50) do
    response = call(kafka, request)
    [%{partition_responses: [partition]}] = response.responses

    if partition.error_code in [3, 6] and retries > 0 do
      Process.sleep(100)
      produce(kafka, request, retries - 1)
    else
      response
    end
  end

  @doc "The topic ID (16-byte binary) of `topic`, via a JSON-compiled Metadata v12"
  @spec topic_id(Testcontainers.Container.t(), String.t()) :: <<_::128>>
  def topic_id(kafka, topic) do
    module = metadata_request_module()

    request =
      struct(module,
        topics: [%{topic_id: nil, name: topic}],
        allow_auto_topic_creation: false,
        include_topic_authorized_operations: false
      )

    %{topics: [%{name: ^topic, error_code: 0, topic_id: topic_id}]} = call(kafka, request)
    assert topic_id != <<0::128>>
    topic_id
  end

  defp metadata_request_module do
    module = GeneratedCode.module(@namespace, :metadata, @metadata_vsn, Request)

    unless Code.ensure_loaded?(module) do
      # Spec sanity: v12 is stable in the vendored specs
      spec = JsonSpec.load!(ApiTable.spec_dir())
      true = JsonSpec.max_stable_version(spec, :metadata) >= @metadata_vsn
      GeneratedCode.compile!(:metadata, MetadataSchema, @namespace)
    end

    module
  end
end
