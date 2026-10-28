defmodule Mix.Tasks.Kayrock.FetchKafkaMessages do
  @moduledoc """
  Fetches Apache Kafka's JSON message definitions for a release tag and
  vendors them into `priv/kafka_messages/<tag>/`.

      mix kayrock.fetch_kafka_messages 4.4.0

  The files come from
  `https://github.com/apache/kafka/tree/<tag>/clients/src/main/resources/common/message`.
  Only the request, response and header specs are copied (plus the upstream
  `README.md` describing the format and the project `NOTICE`), unmodified, so
  that their Apache License headers are preserved. A `PROVENANCE.md` file
  records the tag and the exact upstream commit.

  Requires `git` on the `PATH`. Options:

    * `--repo URL` - clone from a different repository (default:
      `https://github.com/apache/kafka.git`)
    * `--dest DIR` - parent directory to write into (default: `priv/kafka_messages`)

  After fetching a new tag, point `Kayrock.Generate.ApiTable` at it and run
  `mix gen.kafka_protocol`.

  This task is only meant to be run when developing Kayrock.
  """

  use Mix.Task

  @shortdoc "Vendors Apache Kafka's JSON message specs for a release tag"

  @default_repo "https://github.com/apache/kafka.git"
  @message_path "clients/src/main/resources/common/message"

  @impl Mix.Task
  def run(args) do
    {opts, argv} = OptionParser.parse!(args, strict: [repo: :string, dest: :string])

    tag =
      case argv do
        [tag] -> tag
        _ -> Mix.raise("Usage: mix kayrock.fetch_kafka_messages TAG [--repo URL] [--dest DIR]")
      end

    repo = Keyword.get(opts, :repo, @default_repo)
    dest = Path.join(Keyword.get(opts, :dest, "priv/kafka_messages"), tag)

    tmp =
      Path.join(System.tmp_dir!(), "kayrock_kafka_messages_#{System.unique_integer([:positive])}")

    try do
      git!([
        "clone",
        "--quiet",
        "--depth",
        "1",
        "--branch",
        tag,
        "--filter=blob:none",
        "--sparse",
        repo,
        tmp
      ])

      git!(["-C", tmp, "sparse-checkout", "set", @message_path])
      {sha, 0} = System.cmd("git", ["-C", tmp, "rev-parse", "HEAD"])

      source = Path.join(tmp, @message_path)

      files =
        source
        |> File.ls!()
        |> Enum.filter(&vendored?/1)
        |> Enum.sort()

      File.rm_rf!(dest)
      File.mkdir_p!(dest)

      Enum.each(files, &File.cp!(Path.join(source, &1), Path.join(dest, &1)))
      File.cp!(Path.join(tmp, "NOTICE"), Path.join(dest, "NOTICE"))
      File.write!(Path.join(dest, "PROVENANCE.md"), provenance(tag, repo, String.trim(sha)))

      Mix.shell().info("Vendored #{length(files)} files from #{repo} #{tag} into #{dest}")
    after
      File.rm_rf!(tmp)
    end
  end

  @doc false
  def vendored?(file) do
    file == "README.md" or
      String.ends_with?(file, ["Request.json", "Response.json", "Header.json"])
  end

  defp git!(args) do
    case System.cmd("git", args, stderr_to_stdout: true) do
      {_, 0} -> :ok
      {out, status} -> Mix.raise("git #{Enum.join(args, " ")} failed (#{status}):\n#{out}")
    end
  end

  defp provenance(tag, repo, sha) do
    """
    # Apache Kafka message specs #{tag}

    These files are unmodified copies of Apache Kafka's protocol message
    definitions, licensed under the Apache License, Version 2.0 (see the header
    of each file and `NOTICE`).

    - Source: #{repo}
    - Tag: `#{tag}`
    - Commit: `#{sha}`
    - Path: `#{@message_path}`
    - Files: every `*Request.json`, `*Response.json` and `*Header.json`, plus the
      upstream `README.md` describing the format.

    Regenerate with:

        mix kayrock.fetch_kafka_messages #{tag}

    Kayrock reads these with `Kayrock.Generate.JsonSpec`; see
    `Kayrock.Generate.ApiTable` for which APIs and versions are generated from them.
    """
  end
end
