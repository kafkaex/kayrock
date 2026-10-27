defmodule Mix.Tasks.Gen.KafkaProtocol do
  @moduledoc """
  Mix task to generate the .ex files for each of the kafka protocol messages.

  APIs and versions are selected by `Kayrock.Generate.ApiTable`; schemas are
  provided by `Kayrock.Generate.SchemaSource` (`:kpro_schema` for versions
  Kayrock has always generated, the vendored Apache Kafka JSON specs for
  newer versions).

  This task is only meant to be run when developing Kayrock.

  ## Options

    * `--check` - generate into a temporary directory and compare it with the
      checked-in `lib/generated` instead of overwriting it. Exits non-zero
      (listing the differing files) if they are not byte-identical. Used in CI
      to guarantee that generator changes never silently alter generated code.
  """

  use Mix.Task

  require Logger

  alias Kayrock.Generate.SchemaSource

  @output_dir "lib/generated"

  @shortdoc "Generates code for the protocol"
  def run(args) do
    {opts, _, _} = OptionParser.parse(args, strict: [check: :boolean])

    Mix.Task.run("compile")

    case Code.ensure_compiled(:kpro_schema) do
      {:module, _} ->
        :ok

      _ ->
        IO.puts(
          ":kpro_schema is not loaded.  " <>
            "Note, this task should only be run when developing Kayrock."
        )

        exit({:shutdown, 1})
    end

    SchemaSource.validate!()

    if opts[:check] do
      check()
    else
      generate(@output_dir)
    end
  end

  defp generate(output_dir) do
    File.rm_rf!(output_dir)
    File.mkdir_p!(output_dir)

    ast = Kayrock.Generate.generate_schema_metadata(SchemaSource)
    write_ast(ast, Path.join(output_dir, "kafka_schema_metadata.ex"))

    for api <- SchemaSource.all_apis() do
      Logger.info("Generating modules for #{api}")

      ast = Kayrock.Generate.build_all(api, SchemaSource)

      write_ast(ast, Path.join(output_dir, "#{api}.ex"))
    end

    Mix.Task.rerun("format", [Path.join(output_dir, "*.ex")])
  end

  defp check do
    tmp_dir =
      Path.join(System.tmp_dir!(), "kayrock_gen_check_#{System.unique_integer([:positive])}")

    try do
      generate(tmp_dir)

      case diff_dirs(@output_dir, tmp_dir) do
        [] ->
          Mix.shell().info("#{@output_dir} is up to date with the generator.")

        diffs ->
          Mix.raise("""
          #{@output_dir} is out of date with the generator. Differing files:

          #{Enum.map_join(diffs, "\n", &"  * #{&1}")}

          Run `mix gen.kafka_protocol` and commit the result. If an existing
          version's module changed, that is a backward-compatibility break.
          """)
      end
    after
      File.rm_rf!(tmp_dir)
    end
  end

  defp diff_dirs(expected_dir, actual_dir) do
    expected = MapSet.new(File.ls!(expected_dir))
    actual = MapSet.new(File.ls!(actual_dir))

    expected
    |> MapSet.union(actual)
    |> Enum.sort()
    |> Enum.flat_map(fn file ->
      cond do
        not MapSet.member?(actual, file) ->
          ["#{file} (no longer generated)"]

        not MapSet.member?(expected, file) ->
          ["#{file} (generated but not checked in)"]

        File.read!(Path.join(expected_dir, file)) != File.read!(Path.join(actual_dir, file)) ->
          [file]

        true ->
          []
      end
    end)
  end

  defp write_ast(ast, path) do
    code = Macro.to_string(ast)
    File.write!(path, code)
  end
end
