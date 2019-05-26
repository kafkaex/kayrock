defmodule Mix.Tasks.Gen.KafkaProtocol do
  @moduledoc """
  Mix task to generate the .ex files for each of the kafka protocol messages
  """

  use Mix.Task

  require Logger

  @shortdoc "Generates code for the protocol"
  def run(_) do
    output_dir = "lib/generated"

    File.rm_rf!(output_dir)
    File.mkdir_p!(output_dir)

    for api <- :kpro_schema.all_apis() do
      Logger.info("Generating modules for #{api}")

      ast = Kayrock.Generate.build_all(api)

      path = Path.join(output_dir, "#{api}.ex")

      code = Macro.to_string(ast)
      File.write!(path, code)
    end

    Mix.Task.run("format", [Path.join(output_dir, "*.ex")])
  end
end
