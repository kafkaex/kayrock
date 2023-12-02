defmodule Kayrock.MixProject do
  use Mix.Project

  @source_url "https://github.com/dantswain/kayrock"

  def project do
    [
      app: :kayrock,
      version: "0.1.15",
      elixir: "~> 1.1",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mix],
        flags: [:error_handling, :race_conditions]
      ],
      description: "Elixir interface to the Kafka protocol",
      package: package(),
      docs: [
        main: "readme",
        extras: ["README.md"],
        source_url: @source_url,
        skip_undefined_reference_warnings_on: [
          "README.md",
          "lib/kayrock/error_code.ex",
          "lib/kayrock/socket.ex"
        ]
      ]
    ]
  end

  def application do
    [
      extra_applications: [:logger]
    ]
  end

  defp deps do
    [
      # Core
      {:crc32cer, "~>0.1.8"},
      {:varint, "~>1.2"},
      {:connection, "~>1.1.0"},

      # Dev/Test
      {:credo, "~> 1.7", only: [:dev, :test], runtime: false},
      {:dialyxir, "~> 1.4", only: [:dev], runtime: false},
      {:ex_doc, "~> 0.30", only: [:dev], runtime: false},
      {:excoveralls, "~> 0.18", only: :test},
      {:kafka_protocol, "~> 2.4.1", only: [:dev, :test]},
      {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif", only: [:dev, :test]},
      {:snappyer, "~> 1.2", only: [:dev, :test]}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "generated_code"]
  defp elixirc_paths(_), do: ["lib"]

  defp package do
    [
      maintainers: ["Dan Swain"],
      files: ["lib", "config/config.exs", "mix.exs", "README.md"],
      licenses: ["MIT"],
      links: %{"GitHub" => @source_url}
    ]
  end
end
