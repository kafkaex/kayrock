defmodule Mix.Tasks.Gen.KafkaProtocol do
  @moduledoc """
  Mix task to generate the .ex files for each of the kafka protocol messages
  """

  use Mix.Task

  require Logger

  @shortdoc "Generates code for the protocol"
  def run(_) do
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

    output_dir = "lib/generated"

    File.rm_rf!(output_dir)
    File.mkdir_p!(output_dir)

    ast = Kayrock.Generate.generate_schema_metadata(:kpro_schema)
    write_ast(ast, Path.join(output_dir, "kafka_schema_metadata.ex"))

    for api <- all_apis(:kpro_schema) do
      Logger.info("Generating modules for #{api}")

      ast = Kayrock.Generate.build_all(api, :kpro_schema)

      write_ast(ast, Path.join(output_dir, "#{api}.ex"))
    end

    Mix.Task.run("format", [Path.join(output_dir, "*.ex")])
  end

  defp write_ast(ast, path) do
    code = Macro.to_string(ast)
    File.write!(path, code)
  end

  # this slight indirection avoids a compiler warning in user modules that don't
  # have `:kpro_schema`.
  defp all_apis(module) do
    module.all_apis()
  end
end
