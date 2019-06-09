defmodule Kayrock.MixProject do
  use Mix.Project

  def project do
    [
      app: :kayrock,
      version: "0.1.0",
      elixir: "~> 1.6",
      elixirc_paths: elixirc_paths(Mix.env()),
      test_coverage: [tool: ExCoveralls],
      preferred_cli_env: [coveralls: :test],
      start_permanent: Mix.env() == :prod,
      deps: deps(),
      dialyzer: [
        plt_add_apps: [:mix],
        flags: [:error_handling, :race_conditions]
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
      {:kafka_protocol, "~> 2.2.7"},
      {:varint, "~>1.2.0"},
      {:connection, "~>1.0.4"},
      {:credo, "~>1.0.5", only: [:dev, :test], runtime: false},
      {:excoveralls, "~>0.11.1", only: :test},
      {:snappy, git: "https://github.com/fdmanana/snappy-erlang-nif", only: [:dev, :test]},
      {:dialyxir, "~> 1.0.0-rc.6", only: [:dev], runtime: false},
      {:ex_doc, "~>0.20.2", only: [:dev], runtime: false}
    ]
  end

  defp elixirc_paths(:test), do: ["lib", "test/support"]
  defp elixirc_paths(:dev), do: ["lib", "generated_code"]
  defp elixirc_paths(_), do: ["lib"]
end
