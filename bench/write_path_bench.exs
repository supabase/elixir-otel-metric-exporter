# Run with: BENCH_TAG=<label> mix run bench/write_path_bench.exs
#
# Saves results to bench/.benchee/write_path.<tag>. Any existing files matching
# bench/.benchee/write_path.* are loaded for cross-run comparison. Defaults to
# the tag "current" if BENCH_TAG is unset.

Code.require_file("support.exs", __DIR__)

alias OtelMetricExporter.BenchSupport
alias Telemetry.Metrics

# Per-metric-type write hot path. We call OtelMetricExporter.handle_metric/4
# directly — that's the function `:telemetry.attach` invokes — so we measure
# the same path Logflare hits per event without going through telemetry's
# dispatch machinery.

default_buckets = [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

low_cardinality_tag_keys = [:user_id, :source_id]
logflare_tag_keys = [:user_id, :source_id, :tenant, :region]

build_metric = fn
  :counter ->
    Metrics.counter("bench.write.counter")

  :sum ->
    Metrics.sum("bench.write.sum", measurement: :value)

  :last_value ->
    Metrics.last_value("bench.write.last_value", measurement: :value)

  :distribution ->
    Metrics.distribution("bench.write.distribution",
      measurement: :value,
      reporter_options: [buckets: default_buckets]
    )
end

build_metadata = fn
  :low_cardinality_2_keys ->
    %{user_id: "u_42", source_id: "s_7"}

  :logflare_4_keys ->
    %{user_id: "u_42", source_id: "s_7", tenant: "t_1", region: "us-east-1"}
end

# Strip metric struct fields so handle_metric/4 stays on the simple path.
ensure_tag_values_and_keep = fn metric, tag_keys ->
  metric
  |> Map.put(:keep, nil)
  |> Map.put(:tags, tag_keys)
  |> Map.put(:tag_values, fn meta -> meta end)
end

inputs =
  for type <- [:counter, :sum, :last_value, :distribution],
      tag_shape <- [:low_cardinality_2_keys, :logflare_4_keys],
      into: %{} do
    label = "#{type} / #{tag_shape}"

    state_name = :"bench_write_#{type}_#{tag_shape}"

    tag_keys =
      case tag_shape do
        :low_cardinality_2_keys -> low_cardinality_tag_keys
        :logflare_4_keys -> logflare_tag_keys
      end

    metric =
      build_metric.(type)
      |> ensure_tag_values_and_keep.(tag_keys)

    state = BenchSupport.build_state(name: state_name, metrics: [metric])

    metric_config = %{
      metrics: [metric],
      name: state_name,
      extract_tags: nil
    }

    event_name = [:bench, :write, type]

    measurements =
      case type do
        :counter -> %{value: 1}
        _ -> %{value: 42}
      end

    metadata = build_metadata.(tag_shape)

    {label, {state, metric_config, event_name, measurements, metadata}}
  end

job = fn {_state, metric_config, event_name, measurements, metadata} ->
  OtelMetricExporter.handle_metric(event_name, measurements, metadata, metric_config)
end

tag = System.get_env("BENCH_TAG", "current")
save_path = "bench/.benchee/write_path.#{tag}"

prior_runs =
  "bench/.benchee/write_path.*"
  |> Path.wildcard()
  |> Enum.reject(&(&1 == save_path))

Benchee.run(
  %{"handle_metric" => job},
  inputs: inputs,
  time: 5,
  warmup: 2,
  print: [fast_warning: false],
  load: prior_runs,
  save: [path: save_path, tag: tag]
)

# Cleanup
for {_label, {state, _, _, _, _}} <- inputs do
  BenchSupport.cleanup(state)
end
