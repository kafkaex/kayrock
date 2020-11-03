defmodule Kayrock.MixProject do
  use Mix.Project

  def project do
    [
      app: :kayrock,
      version: "0.1.13",
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
        source_url: "https://github.com/kafkaex/kafka_ex"
      ]
    ]
  end

  # Run "mix help compile.app" to learn about applications.
  def application do
    [
      extra_applications: [:logger]
    ]
  end

  # Run "mix help deps" to learn about dependencies.
  defp deps do
    [
      {:crc32cer, "~>0.1.8"},
      {:varint, "~>1.2.0"},
      {:connection, "~>1.0.4"},
      {:kafka_protocol, "~> 2.4.1", only: [:dev, :test]},
      {:credo, "~>1.5.0", only: [:dev, :test], runtime: false},
      {:excoveralls, "~>0.13.3", only: :test},
      {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif", only: [:dev, :test]},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      {:ex_doc, "~>0.23.0", only: [:dev], runtime: false}
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
      links: %{"Github" => "https://github.com/dantswain/kayrock"}
    ]
  end
end
