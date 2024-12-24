defmodule OtelMetricExporter.MixProject do
  use Mix.Project

  def project do
    [
      app: :otel_metric_exporter,
      version: "0.1.0",
      elixir: "~> 1.17",
      start_permanent: Mix.env() == :prod,
      source_url: "https://github.com/electric-sql/elixir-otel-metric-exporter",
      homepage_url: "https://github.com/electric-sql/elixir-otel-metric-exporter",
      deps: deps(),
      docs: &docs/0
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
      {:protobuf, "~> 0.13.0"},
      {:telemetry, "~> 1.3"},
      {:telemetry_metrics, "~> 1.0"},
      {:jason, "~> 1.4"},
      {:nimble_options, "~> 1.1"},
      {:finch, "~> 0.19"},
      {:bypass, "~> 2.1", only: [:test]},
      {:ex_doc, ">= 0.0.0", only: [:dev], runtime: false}
    ]
  end

  defp docs do
    [
      main: "OtelMetricExporter",
      api_reference: false
    ]
  end
end
