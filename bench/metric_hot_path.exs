alias OtelMetricExporter.{HandlerConfig, MetricStore}
alias Telemetry.Metrics

metrics = [
  Metrics.counter("bench.event.count", tags: [:route, :method]),
  Metrics.sum("bench.event.payload_size", tags: [:route, :method]),
  Metrics.last_value("bench.event.queue_depth", tags: [:route]),
  Metrics.distribution("bench.event.duration",
    tags: [:route, :method],
    reporter_options: [buckets: [0, 10, 100, 1_000, 10_000]]
  )
]

measurements = %{
  count: 1,
  payload_size: 512,
  queue_depth: 42,
  duration: 350
}

metadata = %{route: "/users/:id", method: "GET", ignored: "value"}
event = [:bench, :event]

base_config = %{
  otlp_endpoint: "http://localhost:4318",
  export_period: :timer.hours(1),
  metrics: metrics,
  retry: false
}

start_store = fn name, extra ->
  config =
    base_config
    |> Map.merge(extra)
    |> Map.put(:name, name)

  {:ok, _pid} = MetricStore.start_link(config)
  HandlerConfig.compile(metrics, config)
end

original_name = :metric_hot_path_original
{:ok, _pid} = MetricStore.start_link(Map.put(base_config, :name, original_name))

striped_config = start_store.(:metric_hot_path_striped, %{storage: :striped})
original_config = %{metrics: metrics, name: original_name}

parallel = System.get_env("BENCH_PARALLEL", "1") |> String.to_integer()

Benchee.run(
  %{
    "original" => fn ->
      OtelMetricExporter.handle_metric(event, measurements, metadata, original_config)
    end,
    "metric ids / striped ets" => fn ->
      OtelMetricExporter.handle_metric(event, measurements, metadata, striped_config)
    end
  },
  time: 5,
  warmup: 2,
  memory_time: 2,
  parallel: parallel
)
