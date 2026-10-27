defmodule Kayrock.Generate.JsonSpec do
  @moduledoc """
  Translates Apache Kafka's JSON message definitions into Kayrock schemas.

  Apache Kafka describes every protocol message in a JSON file under
  `clients/src/main/resources/common/message/` (vendored into Kayrock under
  `priv/kafka_messages/<kafka-version>/`, see `mix kayrock.fetch_kafka_messages`).
  This module reads those files and produces, for a given API and version, a
  schema in the same shape `:kpro_schema.req/2` and `:kpro_schema.rsp/2`
  return, so that `Kayrock.Generate` can consume either source unchanged.

  ## Translation rules

  For a message at version `v` (flexible when `v` is in the message's
  `flexibleVersions`):

    * a field is present when `v` is in its `versions` and is *not* in its
      `taggedVersions` (tagged fields are carried in the `tagged_fields` bucket
      as raw `{tag, binary}` pairs, exactly as today)
    * field names are `Macro.underscore/1` of the JSON name, as atoms
    * `bool` -> `:boolean`; `int8`/`int16`/`int32`/`int64`/`uint16`/`float64`/`uuid`
      map to the atom of the same name
    * `string` -> `:string` / `:nullable_string` (`:compact_string` /
      `:compact_nullable_string` in flexible versions); a field-level
      `"flexibleVersions": "none"` (RequestHeader `ClientId`) forces the
      non-compact form
    * `bytes` -> `:bytes` / `:nullable_bytes` (compact variants in flexible versions)
    * `records` -> `:records` (`:compact_records` in flexible versions)
    * `[]T` -> `{:array, T}` (`{:compact_array, T}` in flexible versions), where
      `T` is a primitive type or a struct schema
    * a struct-typed field (inline `fields` or a `commonStructs` entry) becomes
      a nested keyword-list schema; when it is nullable in `v` it becomes
      `{:nullable_struct, schema}`
    * in flexible versions every struct (the message itself, array elements and
      nested structs) ends with `{:tagged_fields, :tagged_fields}`

  ## Headers

  The request header is v2 for flexible request versions and v1 otherwise
  (header v0 was only used by `ControlledShutdown` v0, which Kafka 4.x removed
  and Kayrock never generated).
  The response header is v1 for flexible versions and v0 otherwise, except
  `ApiVersions`, whose response always uses header v0 (KIP-511).
  """

  # Jason is a dev/test-only dependency; this module is only used by the
  # code generator (`mix gen.kafka_protocol`), never at runtime.
  @compile {:no_warn_undefined, Jason}

  @typedoc "A parsed version range; `:none` means no versions"
  @type version_range :: {non_neg_integer(), non_neg_integer() | :infinity} | :none

  @typedoc "A Kayrock (kpro-compatible) schema"
  @type schema :: [{atom(), term()}]

  @typedoc "A parsed message spec (request, response or header)"
  @type message :: %{
          name: String.t(),
          type: :request | :response | :header,
          api_key: non_neg_integer() | nil,
          valid_versions: version_range(),
          flexible_versions: version_range(),
          latest_version_unstable: boolean(),
          fields: [map()],
          common_structs: %{optional(String.t()) => map()}
        }

  @typedoc "All messages for one API"
  @type api_spec :: %{
          api: atom(),
          api_key: non_neg_integer(),
          request: message(),
          response: message()
        }

  @typedoc "A loaded spec directory"
  @type t :: %{
          dir: Path.t(),
          apis: %{optional(atom()) => api_spec()},
          request_header: message() | nil,
          response_header: message() | nil
        }

  @primitive_types %{
    "bool" => :boolean,
    "int8" => :int8,
    "int16" => :int16,
    "int32" => :int32,
    "int64" => :int64,
    "uint16" => :uint16,
    "float64" => :float64,
    "uuid" => :uuid
  }

  ##########################################################################
  # Loading

  @doc """
  Loads (and memoizes) every request/response/header JSON spec in `dir`.
  """
  @spec load!(Path.t()) :: t()
  def load!(dir) do
    dir = Path.expand(dir)
    key = {__MODULE__, dir}

    case :persistent_term.get(key, nil) do
      nil ->
        spec = do_load!(dir)
        :persistent_term.put(key, spec)
        spec

      spec ->
        spec
    end
  end

  defp do_load!(dir) do
    messages =
      dir
      |> Path.join("*.json")
      |> Path.wildcard()
      |> Enum.sort()
      |> Enum.map(&parse_file!/1)

    by_type = Enum.group_by(messages, & &1.type)

    requests = Map.new(Map.get(by_type, :request, []), &{&1.api_key, &1})
    responses = Map.new(Map.get(by_type, :response, []), &{&1.api_key, &1})
    headers = Map.new(Map.get(by_type, :header, []), &{&1.name, &1})

    apis =
      Map.new(requests, fn {api_key, request} ->
        response =
          Map.get(responses, api_key) ||
            raise ArgumentError, "No response spec for #{request.name} (api key #{api_key})"

        api = api_name(request.name)
        {api, %{api: api, api_key: api_key, request: request, response: response}}
      end)

    %{
      dir: dir,
      apis: apis,
      request_header: Map.get(headers, "RequestHeader"),
      response_header: Map.get(headers, "ResponseHeader")
    }
  end

  @doc "Parses a single (commented) JSON message spec file"
  @spec parse_file!(Path.t()) :: message()
  def parse_file!(path) do
    path |> File.read!() |> parse!()
  rescue
    e in [Jason.DecodeError, ArgumentError, KeyError] ->
      reraise ArgumentError, "Unable to parse #{path}: #{Exception.message(e)}", __STACKTRACE__
  end

  @doc "Parses the contents of a (commented) JSON message spec"
  @spec parse!(binary()) :: message()
  def parse!(contents) do
    json = contents |> strip_comments() |> Jason.decode!()

    %{
      name: Map.fetch!(json, "name"),
      type: String.to_atom(Map.fetch!(json, "type")),
      api_key: Map.get(json, "apiKey"),
      valid_versions: parse_versions(Map.fetch!(json, "validVersions")),
      flexible_versions: parse_versions(Map.get(json, "flexibleVersions", "none")),
      latest_version_unstable: Map.get(json, "latestVersionUnstable", false),
      fields: Map.get(json, "fields", []),
      common_structs: Map.new(Map.get(json, "commonStructs", []), &{Map.fetch!(&1, "name"), &1})
    }
  end

  @doc """
  Removes `//` line comments (outside of string literals) from a JSON spec.

  Kafka's message specs are JSON with C++-style line comments.
  """
  @spec strip_comments(binary()) :: binary()
  def strip_comments(contents) do
    contents |> do_strip(:code, []) |> Enum.reverse() |> IO.iodata_to_binary()
  end

  defp do_strip(<<>>, _state, acc), do: acc
  defp do_strip(<<"//", rest::binary>>, :code, acc), do: do_strip(rest, :comment, acc)
  defp do_strip(<<?", rest::binary>>, :code, acc), do: do_strip(rest, :string, [?" | acc])
  defp do_strip(<<?\n, rest::binary>>, :comment, acc), do: do_strip(rest, :code, [?\n | acc])
  defp do_strip(<<_, rest::binary>>, :comment, acc), do: do_strip(rest, :comment, acc)

  defp do_strip(<<?\\, c, rest::binary>>, :string, acc),
    do: do_strip(rest, :string, [c, ?\\ | acc])

  defp do_strip(<<?", rest::binary>>, :string, acc), do: do_strip(rest, :code, [?" | acc])
  defp do_strip(<<c, rest::binary>>, state, acc), do: do_strip(rest, state, [c | acc])

  ##########################################################################
  # Versions

  @doc """
  Parses a Kafka version range string (`"0+"`, `"1-3"`, `"5"`, `"none"`).
  """
  @spec parse_versions(String.t()) :: version_range()
  def parse_versions("none"), do: :none

  def parse_versions(str) when is_binary(str) do
    str = String.trim(str)

    cond do
      String.ends_with?(str, "+") ->
        {String.to_integer(String.trim_trailing(str, "+")), :infinity}

      String.contains?(str, "-") ->
        [lo, hi] = String.split(str, "-")
        {String.to_integer(lo), String.to_integer(hi)}

      true ->
        v = String.to_integer(str)
        {v, v}
    end
  end

  @doc "Returns true if `vsn` falls within `range`"
  @spec in_range?(version_range() | String.t() | nil, non_neg_integer()) :: boolean()
  def in_range?(nil, _vsn), do: false
  def in_range?(range, vsn) when is_binary(range), do: in_range?(parse_versions(range), vsn)
  def in_range?(:none, _vsn), do: false
  def in_range?({lo, :infinity}, vsn), do: vsn >= lo
  def in_range?({lo, hi}, vsn), do: vsn >= lo and vsn <= hi

  ##########################################################################
  # Per-API queries

  @doc "Returns the sorted list of API names defined by the spec"
  @spec all_apis(t()) :: [atom()]
  def all_apis(spec) do
    spec.apis |> Map.values() |> Enum.sort_by(& &1.api_key) |> Enum.map(& &1.api)
  end

  @doc "Returns the spec for an API, raising if it is unknown"
  @spec api!(t(), atom()) :: api_spec()
  def api!(spec, api) do
    Map.get(spec.apis, api) || raise ArgumentError, "Unknown API #{inspect(api)} in #{spec.dir}"
  end

  @doc "Returns the Kafka API key for an API"
  @spec api_key(t(), atom()) :: non_neg_integer()
  def api_key(spec, api), do: api!(spec, api).api_key

  @doc """
  Returns the `{min, max}` versions defined by the spec.

  The request and response specs always declare the same `validVersions`.
  """
  @spec valid_versions(t(), atom()) :: {non_neg_integer(), non_neg_integer()} | :none
  def valid_versions(spec, api) do
    %{request: req, response: rsp} = api!(spec, api)

    if req.valid_versions != rsp.valid_versions do
      raise ArgumentError, "#{req.name} and #{rsp.name} disagree on validVersions"
    end

    req.valid_versions
  end

  @doc """
  Returns the highest *stable* version of an API (`nil` if it has none).

  When either the request or the response sets `"latestVersionUnstable": true`
  the highest declared version is not yet stable and is excluded.
  """
  @spec max_stable_version(t(), atom()) :: non_neg_integer() | nil
  def max_stable_version(spec, api) do
    %{request: req, response: rsp} = api!(spec, api)

    case valid_versions(spec, api) do
      # APIs removed by Kafka 4.x (KIP-896) declare validVersions "none"
      :none ->
        nil

      {_min, max} ->
        if req.latest_version_unstable or rsp.latest_version_unstable, do: max - 1, else: max
    end
  end

  @doc "Returns true if `vsn` of `api` is a flexible (KIP-482) version"
  @spec flexible?(t(), atom(), non_neg_integer()) :: boolean()
  def flexible?(spec, api, vsn) do
    in_range?(api!(spec, api).request.flexible_versions, vsn)
  end

  @doc """
  Returns the lowest flexible version of an API, or `nil` if it has none.
  """
  @spec min_flexible_version(t(), atom()) :: non_neg_integer() | nil
  def min_flexible_version(spec, api) do
    case api!(spec, api).request.flexible_versions do
      :none -> nil
      {lo, _} -> lo
    end
  end

  @doc "Request header version used by `vsn` of `api`"
  @spec request_header_version(t(), atom(), non_neg_integer()) :: 1 | 2
  def request_header_version(spec, api, vsn) do
    if flexible?(spec, api, vsn), do: 2, else: 1
  end

  @doc "Response header version used by `vsn` of `api`"
  @spec response_header_version(t(), atom(), non_neg_integer()) :: 0 | 1
  # KIP-511: ApiVersionsResponse always uses response header v0
  def response_header_version(_spec, :api_versions, _vsn), do: 0

  def response_header_version(spec, api, vsn) do
    if flexible?(spec, api, vsn), do: 1, else: 0
  end

  @doc "Returns the request schema for `vsn` of `api`"
  @spec request_schema(t(), atom(), non_neg_integer()) :: schema()
  def request_schema(spec, api, vsn), do: message_schema(api!(spec, api).request, vsn)

  @doc "Returns the response schema for `vsn` of `api`"
  @spec response_schema(t(), atom(), non_neg_integer()) :: schema()
  def response_schema(spec, api, vsn), do: message_schema(api!(spec, api).response, vsn)

  @doc """
  Translates a parsed message into a schema for version `vsn`.
  """
  @spec message_schema(message(), non_neg_integer()) :: schema()
  def message_schema(message, vsn) do
    unless in_range?(message.valid_versions, vsn) do
      raise ArgumentError,
            "#{message.name} does not define version #{vsn} " <>
              "(validVersions #{inspect(message.valid_versions)})"
    end

    flexible = in_range?(message.flexible_versions, vsn)
    struct_schema(message.fields, vsn, flexible, message.common_structs)
  end

  ##########################################################################
  # Field translation

  defp struct_schema(fields, vsn, flexible, common_structs) do
    translated =
      fields
      |> Enum.filter(&present?(&1, vsn))
      |> Enum.map(&translate_field(&1, vsn, flexible, common_structs))

    if flexible, do: translated ++ [tagged_fields: :tagged_fields], else: translated
  end

  defp present?(field, vsn) do
    in_range?(Map.fetch!(field, "versions"), vsn) and
      not in_range?(Map.get(field, "taggedVersions"), vsn)
  end

  defp translate_field(field, vsn, flexible, common_structs) do
    name = field |> Map.fetch!("name") |> field_name()
    nullable = in_range?(Map.get(field, "nullableVersions"), vsn)

    # a field-level flexibleVersions overrides the message's (only "none" is
    # used in practice: RequestHeader.ClientId)
    field_flexible =
      case Map.fetch(field, "flexibleVersions") do
        {:ok, range} -> in_range?(range, vsn)
        :error -> flexible
      end

    {name, translate_type(field, Map.fetch!(field, "type"), vsn, flexible, field_flexible, nullable, common_structs)}
  end

  defp translate_type(field, "[]" <> elem_type, vsn, flexible, field_flexible, _nullable, common_structs) do
    elem =
      case Map.fetch(@primitive_types, elem_type) do
        {:ok, type} ->
          type

        :error when elem_type in ["string", "bytes", "records"] ->
          # array elements are never nullable
          translate_type(field, elem_type, vsn, flexible, field_flexible, false, common_structs)

        :error ->
          struct_schema(struct_fields!(field, elem_type, common_structs), vsn, flexible, common_structs)
      end

    if flexible, do: {:compact_array, elem}, else: {:array, elem}
  end

  defp translate_type(_field, "string", _vsn, _flexible, field_flexible, nullable, _cs) do
    case {field_flexible, nullable} do
      {false, false} -> :string
      {false, true} -> :nullable_string
      {true, false} -> :compact_string
      {true, true} -> :compact_nullable_string
    end
  end

  defp translate_type(_field, "bytes", _vsn, _flexible, field_flexible, nullable, _cs) do
    case {field_flexible, nullable} do
      {false, false} -> :bytes
      {false, true} -> :nullable_bytes
      {true, false} -> :compact_bytes
      {true, true} -> :compact_nullable_bytes
    end
  end

  defp translate_type(_field, "records", _vsn, _flexible, field_flexible, _nullable, _cs) do
    if field_flexible, do: :compact_records, else: :records
  end

  defp translate_type(field, type, vsn, flexible, _field_flexible, nullable, common_structs) do
    case Map.fetch(@primitive_types, type) do
      {:ok, primitive} ->
        primitive

      :error ->
        schema = struct_schema(struct_fields!(field, type, common_structs), vsn, flexible, common_structs)
        if nullable, do: {:nullable_struct, schema}, else: schema
    end
  end

  defp struct_fields!(field, type, common_structs) do
    case Map.fetch(field, "fields") do
      {:ok, fields} ->
        fields

      :error ->
        case Map.fetch(common_structs, type) do
          {:ok, %{"fields" => fields}} ->
            fields

          _ ->
            raise ArgumentError,
                  "Unknown type #{inspect(type)} for field #{inspect(Map.get(field, "name"))}"
        end
    end
  end

  ##########################################################################
  # Naming

  @doc """
  Converts a message name (`"DescribeGroupsRequest"`) into a Kayrock API name
  (`:describe_groups`).
  """
  @spec api_name(String.t()) :: atom()
  def api_name(message_name) do
    message_name
    |> String.replace_suffix("Request", "")
    |> String.replace_suffix("Response", "")
    |> Macro.underscore()
    |> String.to_atom()
  end

  @doc "Converts a JSON field name (`\"ThrottleTimeMs\"`) to a field atom (`:throttle_time_ms`)"
  @spec field_name(String.t()) :: atom()
  def field_name(name), do: name |> Macro.underscore() |> String.to_atom()
end
