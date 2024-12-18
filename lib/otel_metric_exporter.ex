defmodule OtelMetricExporter do
  @moduledoc """
  This is a `telemetry` exporter that collects specified metrics
  and then exports them to an OTel endpoint. It uses metric definitions
  from `:telemetry_metrics` library.

  Example usage:

      OtelMetricExporter.start_link(
        otlp_protocol: :http_protobuf, # Currently `http_protobuf` and `http_json` are supported
        otlp_endpoint: otlp_endpoint,
        otlp_headers: headers,
        otlp_compression: :gzip,
        export_period: :timer.seconds(30),
        metrics: [
          Telemetry.Metrics.counter("plug.request.stop.duration"),
          Telemetry.Metrics.sum("plug.request.stop.duration"),
          Telemetry.Metrics.last_value("plug.request.stop.duration"),
          Telemetry.Metrics.distribution("plug.request.stop.duration"),
        ]
      )
  """
  use Supervisor
  require Logger
  alias OtelMetricExporter.MetricStore

  @type protocol :: :http_protobuf | :http_json
  @type compression :: :gzip | nil

  @options_schema NimbleOptions.new!([
    otlp_protocol: [
      type: {:in, [:http_protobuf, :http_json]},
      required: true,
      doc: "Protocol to use for OTLP export. Currently only HTTP-based protocols are supported"
    ],
    otlp_endpoint: [
      type: :string,
      required: true,
      doc: "Endpoint to export metrics to"
    ],
    otlp_headers: [
      type: {:map, :string, :string},
      default: %{},
      doc: "Headers to include in the export request"
    ],
    otlp_compression: [
      type: {:in, [:gzip]},
      default: nil,
      doc: "Compression to use for the export request"
    ],
    export_period: [
      type: :pos_integer,
      default: :timer.seconds(30),
      doc: "Period between exports in milliseconds"
    ],
    metrics: [
      type: {:list, :any},
      required: true,
      doc: "List of telemetry metrics to collect"
    ]
  ])

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    with {:ok, validated} <- NimbleOptions.validate(opts, @options_schema) do
      Supervisor.start_link(__MODULE__, validated, name: __MODULE__)
    end
  end

  @impl true
  def init(config) do
    finch_name = Module.concat(__MODULE__, Finch)

    :ok = setup_telemetry_handlers(config)

    children = [
      {Finch,
        name: finch_name,
        pools: %{
          :default => [
            size: 10,
            count: 1
          ]
        }},
      {MetricStore, config}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp setup_telemetry_handlers(config) do
    handlers =
      config.metrics
      |> Enum.map(fn metric ->
        handler_id = {__MODULE__, metric.name}

        :telemetry.attach(
          handler_id,
          metric.event_name,
          &handle_metric/4,
          %{metric: metric}
        )

        handler_id
      end)

    # Store handler IDs in the process dictionary for cleanup
    Process.put(:"$otel_metric_handlers", handlers)

    :ok
  end

  defp handle_metric(_event_name, measurements, metadata, %{metric: metric}) do
    value = extract_measurement(metric, measurements)

    tags = extract_tags(metric, metadata)

    GenServer.cast(MetricStore, {:record_metric, metric.name, value, tags})
  end

  defp extract_measurement(metric, measurements) do
    case metric.measurement do
      fun when is_function(fun, 1) -> fun.(measurements)
      key -> measurements[key]
    end
  end

  defp extract_tags(metric, metadata) do
    metric.tags
    |> Enum.map(fn
      {tag_key, {module, fun}} -> {tag_key, apply(module, fun, [metadata])}
      {tag_key, fun} when is_function(fun, 1) -> {tag_key, fun.(metadata)}
      {tag_key, tag_value} -> {tag_key, tag_value}
    end)
    |> Map.new()
  end
end
