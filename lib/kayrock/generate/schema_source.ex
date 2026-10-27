defmodule Kayrock.Generate.SchemaSource do
  @moduledoc """
  The schema source used by `mix gen.kafka_protocol`.

  It exposes the same functions as `:kpro_schema` (`all_apis/0`,
  `vsn_range/1`, `min_flexible_vsn/1`, `api_key/1`, `req/2`, `rsp/2`, `ec/1`),
  so it can be passed anywhere `Kayrock.Generate` accepts a schema module.

  Which APIs and versions exist is decided by `Kayrock.Generate.ApiTable`:

    * versions `<= ApiTable.kpro_max_version(api)` are read from `:kpro_schema`
      (keeping already-generated code byte-identical);
    * higher versions, up to `ApiTable.max_version(api)`, are translated from
      the vendored Apache Kafka JSON specs by `Kayrock.Generate.JsonSpec`.

  `validate!/0` checks the table against both sources and is run before any
  code is generated.
  """

  alias Kayrock.Generate.ApiTable
  alias Kayrock.Generate.JsonSpec

  # :kpro_schema is a dev/test-only dependency
  @compile {:no_warn_undefined, :kpro_schema}

  @no_flexible_versions 9999

  @doc "Loaded JSON spec for the pinned Kafka version"
  @spec json_spec :: JsonSpec.t()
  def json_spec, do: JsonSpec.load!(ApiTable.spec_dir())

  @spec all_apis :: [atom()]
  def all_apis, do: ApiTable.apis()

  @spec vsn_range(atom()) :: {non_neg_integer(), non_neg_integer()}
  def vsn_range(api) do
    min =
      case ApiTable.kpro_max_version(api) do
        nil -> api |> json_valid_versions() |> elem(0)
        _ -> api |> kpro_vsn_range() |> elem(0)
      end

    {min, ApiTable.max_version(api)}
  end

  @doc """
  The lowest flexible version of `api` (`9999` when it has none, matching
  `:kpro_schema.min_flexible_vsn/1`).
  """
  @spec min_flexible_vsn(atom()) :: non_neg_integer()
  def min_flexible_vsn(api) do
    JsonSpec.min_flexible_version(json_spec(), api) || @no_flexible_versions
  end

  @doc "Request header version for `vsn` of `api` (see `Kayrock.Generate.JsonSpec`)"
  @spec request_header_vsn(atom(), non_neg_integer()) :: 1 | 2
  def request_header_vsn(api, vsn), do: JsonSpec.request_header_version(json_spec(), api, vsn)

  @doc "Response header version for `vsn` of `api` (see `Kayrock.Generate.JsonSpec`)"
  @spec response_header_vsn(atom(), non_neg_integer()) :: 0 | 1
  def response_header_vsn(api, vsn), do: JsonSpec.response_header_version(json_spec(), api, vsn)

  @spec api_key(atom()) :: non_neg_integer()
  def api_key(api), do: JsonSpec.api_key(json_spec(), api)

  @spec req(atom(), non_neg_integer()) :: JsonSpec.schema()
  def req(api, vsn) do
    case source(api, vsn) do
      :kpro -> :kpro_schema.req(api, vsn)
      :json -> JsonSpec.request_schema(json_spec(), api, vsn)
    end
  end

  @spec rsp(atom(), non_neg_integer()) :: JsonSpec.schema()
  def rsp(api, vsn) do
    case source(api, vsn) do
      :kpro -> :kpro_schema.rsp(api, vsn)
      :json -> JsonSpec.response_schema(json_spec(), api, vsn)
    end
  end

  @doc "Error code to error atom (from `:kpro_schema`)"
  @spec ec(integer()) :: atom()
  def ec(code), do: :kpro_schema.ec(code)

  @doc "Which source a given API version is read from"
  @spec source(atom(), non_neg_integer()) :: :kpro | :json
  def source(api, vsn) do
    {min, max} = vsn_range(api)

    unless vsn >= min and vsn <= max do
      raise ArgumentError,
            "#{api} v#{vsn} is outside the generated range #{min}..#{max} (see Kayrock.Generate.ApiTable)"
    end

    kpro_max = ApiTable.kpro_max_version(api)
    if kpro_max != nil and vsn <= kpro_max, do: :kpro, else: :json
  end

  @doc """
  Validates `Kayrock.Generate.ApiTable` against `:kpro_schema` and the JSON specs.

  Raises `ArgumentError` describing every problem found.
  """
  @spec validate! :: :ok
  def validate! do
    spec = json_spec()

    errors =
      validate_headers(spec) ++
        Enum.flat_map(ApiTable.max_versions(), fn {api, max} ->
          validate_api(spec, api, max)
        end)

    case errors do
      [] -> :ok
      _ -> raise ArgumentError, "Invalid Kayrock.Generate.ApiTable:\n  " <> Enum.join(errors, "\n  ")
    end
  end

  # The header encodings are hardcoded in Kayrock.Generate (request header
  # v0-v2, response header v0-v1); fail loudly if a new spec changes them.
  defp validate_headers(spec) do
    expected = [
      {:request_header, {1, 2}, {2, :infinity}, 1,
       [
         request_api_key: :int16,
         request_api_version: :int16,
         correlation_id: :int32,
         client_id: :nullable_string
       ]},
      {:request_header, {1, 2}, {2, :infinity}, 2,
       [
         request_api_key: :int16,
         request_api_version: :int16,
         correlation_id: :int32,
         client_id: :nullable_string,
         tagged_fields: :tagged_fields
       ]},
      {:response_header, {0, 1}, {1, :infinity}, 0, [correlation_id: :int32]},
      {:response_header, {0, 1}, {1, :infinity}, 1,
       [correlation_id: :int32, tagged_fields: :tagged_fields]}
    ]

    Enum.flat_map(expected, fn {key, valid, flexible, vsn, expected_schema} ->
      case Map.fetch!(spec, key) do
        nil ->
          ["#{key}: missing from #{spec.dir}"]

        %{valid_versions: ^valid, flexible_versions: ^flexible} = header ->
          validate_header_schema(key, header, vsn, expected_schema)

        header ->
          [
            "#{key}: unexpected versions " <>
              inspect({header.valid_versions, header.flexible_versions})
          ]
      end
    end)
  end

  defp validate_header_schema(key, header, vsn, expected_schema) do
    case JsonSpec.message_schema(header, vsn) do
      ^expected_schema -> []
      other -> ["#{key}: v#{vsn} schema changed: #{inspect(other)}"]
    end
  end

  defp validate_api(spec, api, max) do
    cond do
      not Map.has_key?(spec.apis, api) ->
        ["#{api}: not defined in the Kafka #{ApiTable.kafka_spec_version()} JSON specs"]

      ApiTable.kpro_max_version(api) != nil and kpro_vsn_range(api) == false ->
        ["#{api}: listed in kpro_max_versions but unknown to :kpro_schema"]

      true ->
        validate_versions(spec, api, max)
    end
  end

  defp validate_versions(spec, api, max) do
    kpro_max = ApiTable.kpro_max_version(api)
    kpro_errors = validate_kpro_max(api, kpro_max)

    # versions above kpro_max (all versions for JSON-only APIs) come from JSON
    json_errors =
      if kpro_max != nil and max <= kpro_max do
        []
      else
        validate_json_versions(spec, api, max, kpro_max)
      end

    kpro_errors ++ json_errors
  end

  defp validate_kpro_max(_api, nil), do: []

  defp validate_kpro_max(api, kpro_max) do
    case kpro_vsn_range(api) do
      {_, ^kpro_max} ->
        []

      other ->
        ["#{api}: kpro_max_versions says #{kpro_max} but :kpro_schema has #{inspect(other)}"]
    end
  end

  defp validate_json_versions(spec, api, max, kpro_max) do
    stable_max = JsonSpec.max_stable_version(spec, api)
    first_json_vsn = if kpro_max == nil, do: 0, else: kpro_max + 1

    case JsonSpec.valid_versions(spec, api) do
      :none ->
        ["#{api}: removed from the Kafka #{ApiTable.kafka_spec_version()} JSON specs"]

      _ when max > stable_max ->
        ["#{api}: max version #{max} is above the highest stable JSON version #{stable_max}"]

      {json_min, _} when kpro_max != nil and json_min > first_json_vsn ->
        ["#{api}: JSON specs start at v#{json_min}, leaving a gap after kpro's v#{kpro_max}"]

      _ ->
        []
    end
  end

  defp json_valid_versions(api), do: JsonSpec.valid_versions(json_spec(), api)

  defp kpro_vsn_range(api) do
    if Code.ensure_loaded?(:kpro_schema), do: :kpro_schema.vsn_range(api), else: false
  end
end
