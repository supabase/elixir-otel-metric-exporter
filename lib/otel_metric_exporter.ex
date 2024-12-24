defmodule OtelMetricExporter do
  use Supervisor
  require Logger
  alias OtelMetricExporter.MetricStore

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
          Telemetry.Metrics.distribution("plug.request.stop.duration",
            reporter_options: [buckets: [0, 10, 100, 1000]] # Optional histogram buckets.
          ),
        ]
      )

  Default histogram buckets are `#{inspect(MetricStore.default_buckets())}`

  See all available options in `start_link/2` documentation.
  """

  @type protocol :: :http_protobuf | :http_json
  @type compression :: :gzip | nil

  @options_schema NimbleOptions.new!(
                    metrics: [
                      type: {:list, :any},
                      required: true,
                      doc: "List of telemetry metrics to track"
                    ],
                    otlp_protocol: [
                      type: {:in, [:http_protobuf, :http_json]},
                      default: :http_protobuf,
                      doc:
                        "Protocol to use for OTLP export. Currently only :http_protobuf and :http_json are supported"
                    ],
                    otlp_endpoint: [
                      type: :string,
                      required: true,
                      doc: "Endpoint to send metrics to"
                    ],
                    otlp_headers: [
                      type: :map,
                      default: %{},
                      doc: "Headers to send with OTLP requests"
                    ],
                    otlp_compression: [
                      type: {:in, [:gzip, nil]},
                      default: nil,
                      doc: "Compression to use for OTLP requests"
                    ],
                    export_period: [
                      type: :pos_integer,
                      default: :timer.seconds(30),
                      doc: "Period in milliseconds between metric exports"
                    ]
                  )

  @doc """
  Start the exporter. It maintains some pieces of global state: ets table and a `:persistent_term` key.
  This means that only one exporter instance can be started at a time.

  ## Options

  #{NimbleOptions.docs(@options_schema)}
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts) do
    with {:ok, validated} <- NimbleOptions.validate(opts, @options_schema) do
      Supervisor.start_link(__MODULE__, Map.new(validated), name: __MODULE__)
    end
  end

  @impl true
  def init(config) do
    finch_name = Module.concat(__MODULE__, Finch)

    :ok = setup_telemetry_handlers(config)

    children = [
      {Finch, name: finch_name, pools: %{:default => [size: 10, count: 1]}},
      {MetricStore, Map.put(config, :finch_pool, finch_name)}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp setup_telemetry_handlers(config) do
    handlers =
      config.metrics
      |> Enum.group_by(& &1.event_name)
      |> Enum.map(fn {event_name, metrics} ->
        handler_id = {__MODULE__, event_name}

        :telemetry.attach(
          handler_id,
          event_name,
          &__MODULE__.handle_metric/4,
          %{metrics: metrics}
        )

        handler_id
      end)

    # Store handler IDs in the process dictionary for cleanup
    Process.put(:"$otel_metric_handlers", handlers)

    :ok
  end

  @doc false
  def handle_metric(_event_name, measurements, metadata, %{metrics: metrics}) do
    for metric <- metrics do
      if is_nil(metric.keep) || metric.keep.(metadata) do
        value = extract_measurement(metric, measurements, metadata)
        tags = extract_tags(metric, metadata)

        metric_name = "#{Enum.join(metric.name, ".")}"
        MetricStore.write_metric(metric, metric_name, value, tags)
      end
    end
  end

  defp extract_measurement(metric, measurements, metadata) do
    case metric.measurement do
      fun when is_function(fun, 1) -> fun.(measurements)
      fun when is_function(fun, 2) -> fun.(measurements, metadata)
      key -> Map.get(measurements, key)
    end
  end

  defp extract_tags(metric, metadata) do
    metadata
    |> metric.tag_values.()
    |> Map.take(metric.tags)
  end
end
