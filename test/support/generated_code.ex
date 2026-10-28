defmodule Kayrock.Test.GeneratedCode do
  @moduledoc """
  Compiles `Kayrock.Generate` output for test-only schemas.

  Code is generated exactly as `mix gen.kafka_protocol` does (AST -> source),
  then moved into a test namespace, with `api_key/0` resolved through the
  given schema module instead of `Kayrock.KafkaSchemaMetadata`.  Nothing is
  written to `lib/generated`.
  """

  @doc """
  Generates and compiles the modules for `api` from `schema_module` under
  `namespace` (e.g. `MyTest.Gen` gives `MyTest.Gen.Kayrock.Fetch.V12.Request`).
  """
  @spec compile!(atom(), module(), module()) :: :ok
  def compile!(api, schema_module, namespace) do
    root = "Kayrock.#{Macro.camelize("#{api}")}"

    source =
      api
      |> Kayrock.Generate.build_all(schema_module)
      # protocols are consolidated in test, so Kayrock.Request implementations
      # for runtime-compiled structs would have no effect (and warn); drop them
      |> Macro.prewalk(fn
        {:__block__, meta, exprs} -> {:__block__, meta, Enum.reject(exprs, &defimpl?/1)}
        other -> other
      end)
      |> Macro.to_string()
      |> String.replace("defmodule #{root} do", "defmodule #{inspect(namespace)}.#{root} do")
      |> String.replace(
        "Kayrock.KafkaSchemaMetadata.api_key(",
        "#{inspect(schema_module)}.api_key("
      )

    Code.compile_string(source)
    :ok
  end

  @doc "The compiled module for `vsn` of `api` (`kind` is `Request` or `Response`)"
  @spec module(module(), atom(), non_neg_integer(), module()) :: module()
  def module(namespace, api, vsn, kind) do
    Module.concat([namespace, "Kayrock", Macro.camelize("#{api}"), "V#{vsn}", kind])
  end

  @doc """
  Serializes a request and returns its body (everything after the request
  header).  Asserts the header's api key and version.
  """
  @spec request_body(struct()) :: binary()
  def request_body(%module{} = request) do
    wire = IO.iodata_to_binary(module.serialize(%{request | correlation_id: 1, client_id: nil}))
    key = module.api_key()
    vsn = module.api_vsn()
    <<^key::16, ^vsn::16, 1::32, -1::16-signed, rest::binary>> = wire

    # header v2 (flexible versions) ends with an empty tag buffer
    if Keyword.has_key?(module.schema(), :tagged_fields) do
      <<0, body::binary>> = rest
      body
    else
      rest
    end
  end

  defp defimpl?({:defimpl, _, _}), do: true
  defp defimpl?(_), do: false
end
