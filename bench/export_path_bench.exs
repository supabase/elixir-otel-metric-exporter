# Run with: BENCH_TAG=<label> mix run bench/export_path_bench.exs
#
# Saves results to bench/.benchee/export_path.<tag>. Any existing files matching
# bench/.benchee/export_path.* are loaded for cross-run comparison. Defaults to
# the tag "current" if BENCH_TAG is unset.

Code.require_file("support.exs", __DIR__)

alias OtelMetricExporter.BenchSupport
alias OtelMetricExporter.MetricStore
alias Telemetry.Metrics

default_buckets = MetricStore.default_buckets()

# Three shapes, each producing roughly ~100k ETS rows once populated.
shape_specs = %{
  "wide_tag_10m_10kt" => %{
    metrics_fun: fn ->
      for i <- 1..10, do: Metrics.counter("bench.wide.counter.#{i}")
    end,
    shape: [
      metric_count: 10,
      tag_cardinality: 10_000,
      metric_types: [:counter]
    ]
  },
  "tall_metric_100m_1kt" => %{
    metrics_fun: fn ->
      for i <- 1..100, do: Metrics.counter("bench.tall.counter.#{i}")
    end,
    shape: [
      metric_count: 100,
      tag_cardinality: 1_000,
      metric_types: [:counter]
    ]
  },
  "distribution_heavy_4m_25kt" => %{
    metrics_fun: fn ->
      for i <- 1..4 do
        Metrics.distribution("bench.dist.#{i}",
          measurement: :value,
          reporter_options: [buckets: default_buckets]
        )
      end
    end,
    shape: [
      metric_count: 4,
      tag_cardinality: 25_000,
      metric_types: [:distribution]
    ]
  }
}

callback_variants = [
  {:no_op, fn _batch, _config -> :ok end},
  {:logflare_shaped, &BenchSupport.logflare_shaped_callback/2}
]

# Build one state per (shape, callback) input. Population happens lazily via
# `before_each: BenchSupport.repopulate/1` (the state stashes the shape).
inputs =
  for {shape_label, %{metrics_fun: metrics_fun, shape: shape}} <- shape_specs,
      {cb_label, cb_fun} <- callback_variants,
      into: %{} do
    label = "#{shape_label} / #{cb_label}"
    name = :"bench_export_#{shape_label}_#{cb_label}"

    state =
      BenchSupport.build_state(
        name: name,
        metrics: metrics_fun.(),
        export_callback: cb_fun,
        max_batch_size: 500
      )

    # Prime the shape so repopulate/1 knows what to write each iteration.
    state = BenchSupport.populate(state, shape)

    {label, state}
  end

job = fn state ->
  # Drive export_sync directly through handle_call/3 to bypass the GenServer
  # mailbox and measure CPU only.
  {:reply, _result, _new_state} =
    MetricStore.handle_call(:export_sync, {self(), make_ref()}, state)

  :ok
end

tag = System.get_env("BENCH_TAG", "current")
save_path = "bench/.benchee/export_path.#{tag}"

prior_runs =
  "bench/.benchee/export_path.*"
  |> Path.wildcard()
  |> Enum.reject(&(&1 == save_path))

Benchee.run(
  %{"export_sync" => job},
  inputs: inputs,
  before_each: &BenchSupport.repopulate/1,
  time: 8,
  warmup: 2,
  print: [fast_warning: false],
  load: prior_runs,
  save: [path: save_path, tag: tag]
)

for {_label, state} <- inputs do
  BenchSupport.cleanup(state)
end
