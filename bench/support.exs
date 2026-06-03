defmodule OtelMetricExporter.BenchSupport do
  @moduledoc """
  Fixture helpers for benches under `bench/`.

  Builds a `%MetricStore.State{}` directly without going through
  `MetricStore.start_link/1`, so benches can drive `write_metric/5` and the
  export pipeline (via `handle_call(:export_sync, ...)`) synchronously.
  """

  alias OtelMetricExporter.MetricStore
  alias OtelMetricExporter.MetricStore.State
  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.OtelApi.Config
  alias Telemetry.Metrics

  @default_tag_keys [:user_id, :source_id, :tenant, :region]
  @default_metric_types [:counter, :sum, :last_value, :distribution]

  # Mirror the bucket list used by MetricStore for distribution metrics.
  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  @shape_persistent_key {__MODULE__, :populate_shape}

  # Mirrors metric_store.ex:496 generation_key/1
  defp generation_key(metrics_table) do
    {OtelMetricExporter.MetricStore, metrics_table, :generation}
  end

  @doc """
  Build a `%MetricStore.State{}` and the ETS infrastructure it expects.

  Options:

    * `:name` (atom, default `:bench_metrics`) - name of the public ETS table.
    * `:metrics` (list of `Telemetry.Metrics.*` definitions). When omitted, a
      default mixed-type set is built using the metric name `bench.metric`.
    * `:export_callback` (2-arity fun, default no-op) - placed on the OtelApi
      config so `OtelApi.send_metrics/2` routes through the callback path.
    * `:max_batch_size` (default 500).
  """
  def build_state(opts \\ []) do
    name = Keyword.get(opts, :name, :bench_metrics)
    metrics = Keyword.get(opts, :metrics, default_metrics())
    export_callback = Keyword.get(opts, :export_callback, fn _batch, _config -> :ok end)
    max_batch_size = Keyword.get(opts, :max_batch_size, 500)

    # Public ETS table - same flags as metric_store.ex:144
    :ets.new(name, [:ordered_set, :public, :named_table, {:write_concurrency, :auto}])

    # Production uses `:private` here (owned by the GenServer); for benches
    # we need `:public` so Benchee's worker tasks can operate on the table.
    generations_table = :ets.new(:"#{name}_generations", [:ordered_set, :public])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    :persistent_term.put(generation_key(name), 0)

    api = %OtelApi{
      finch: nil,
      retry: false,
      scope: :metrics,
      config: %Config{
        otlp_endpoint: "http://localhost:4318",
        otlp_protocol: :http_protobuf,
        otlp_headers: %{},
        otlp_timeout: 10_000,
        otlp_compression: nil,
        otlp_concurrent_requests: 10,
        exporter: :otlp,
        resource: %{},
        max_batch_size: max_batch_size,
        max_concurrency: System.schedulers_online(),
        max_table_memory: :infinity,
        export_callback: export_callback
      }
    }

    %State{
      config: %{export_period: 600_000},
      api: api,
      metrics: metrics,
      metrics_table: name,
      generations_table: generations_table,
      last_export: nil
    }
  end

  @doc """
  Populate `state` with `MetricStore.write_metric/5` calls.

  `shape` keyword:

    * `:metric_count` - number of distinct metrics per type (default 5).
    * `:tag_cardinality` - number of distinct tag-tuples to emit (default 100).
    * `:tag_keys` - list of atom keys per tag map (default 4-key Logflare-shape).
    * `:metric_types` - subset of `[:counter, :sum, :last_value, :distribution]`.
  """
  def populate(%State{} = state, shape) do
    shape =
      shape
      |> Keyword.put_new(:tag_keys, @default_tag_keys)
      |> Keyword.put_new(:metric_count, 5)
      |> Keyword.put_new(:tag_cardinality, 100)
      |> Keyword.put_new(:metric_types, @default_metric_types)

    # Stash shape so repopulate/1 can replay it.
    :persistent_term.put({@shape_persistent_key, state.metrics_table}, shape)

    metric_types = Keyword.fetch!(shape, :metric_types)
    metric_count = Keyword.fetch!(shape, :metric_count)
    tag_cardinality = Keyword.fetch!(shape, :tag_cardinality)
    tag_keys = Keyword.fetch!(shape, :tag_keys)

    metrics_to_use =
      Enum.filter(state.metrics, fn metric ->
        metric_module_to_type(metric.__struct__) in metric_types
      end)
      |> Enum.take(metric_count * length(metric_types))

    for metric <- metrics_to_use, i <- 0..(tag_cardinality - 1) do
      tags = build_tags(tag_keys, i, tag_cardinality)
      value = pick_value_for(metric, i)
      MetricStore.write_metric(state.metrics_table, metric, value, tags)
    end

    state
  end

  @doc """
  Reset the state's ETS tables and generation counter, then re-run population
  with the same shape that was passed to `populate/2`.
  """
  def repopulate(%State{} = state) do
    :ets.delete_all_objects(state.metrics_table)
    :ets.delete_all_objects(state.generations_table)
    :ets.insert(state.generations_table, {0, System.system_time(:nanosecond), 0})
    :persistent_term.put(generation_key(state.metrics_table), 0)

    case :persistent_term.get({@shape_persistent_key, state.metrics_table}, nil) do
      nil -> state
      shape -> populate(state, shape)
    end
  end

  @doc """
  Drop the ETS tables and erase persistent_term keys associated with `state`.
  """
  def cleanup(%State{} = state) do
    if :ets.info(state.metrics_table) != :undefined do
      :ets.delete(state.metrics_table)
    end

    if :ets.info(state.generations_table) != :undefined do
      :ets.delete(state.generations_table)
    end

    :persistent_term.erase(generation_key(state.metrics_table))
    :persistent_term.erase({@shape_persistent_key, state.metrics_table})
    :ok
  end

  @doc """
  Mimic the Logflare `UserMonitoring.exporter_callback/2` workload: walk every
  metric, walk every data point, allocate a small map per data point, then
  group results by a tag value via `Map.update/4`.

  Returns `:ok`.
  """
  def logflare_shaped_callback({:metrics, metrics}, _config) do
    grouped =
      Enum.reduce(metrics, %{}, fn %{data: {_kind, %{data_points: data_points}}} = metric, acc ->
        Enum.reduce(data_points, acc, fn dp, inner_acc ->
          group_key = group_key_for(dp)

          entry = %{
            metric_name: metric.name,
            attributes: dp.attributes,
            ts: dp.time_unix_nano,
            payload_size: estimate_size(dp)
          }

          Map.update(inner_acc, group_key, [entry], fn list -> [entry | list] end)
        end)
      end)

    # Force the lazy work to actually happen
    _ = map_size(grouped)
    :ok
  end

  defp group_key_for(%{attributes: attributes}) when is_list(attributes) do
    case Enum.find(attributes, fn kv -> kv.key == "user_id" end) do
      nil -> :__none__
      %{value: %{value: {_, v}}} -> v
      _ -> :__none__
    end
  end

  defp group_key_for(_), do: :__none__

  defp estimate_size(%{attributes: attributes}) when is_list(attributes), do: length(attributes)
  defp estimate_size(_), do: 0

  defp default_metrics do
    [
      Metrics.counter("bench.metric.counter"),
      Metrics.sum("bench.metric.sum", measurement: :value),
      Metrics.last_value("bench.metric.last_value", measurement: :value),
      Metrics.distribution("bench.metric.distribution",
        measurement: :value,
        reporter_options: [buckets: @default_buckets]
      )
    ]
  end

  defp metric_module_to_type(Metrics.Counter), do: :counter
  defp metric_module_to_type(Metrics.Sum), do: :sum
  defp metric_module_to_type(Metrics.LastValue), do: :last_value
  defp metric_module_to_type(Metrics.Distribution), do: :distribution

  defp build_tags(tag_keys, i, cardinality) do
    Enum.with_index(tag_keys)
    |> Map.new(fn {key, idx} ->
      # Vary each dimension at a different rate so we get a wide tag-tuple
      # space without exploding cardinality unnecessarily.
      divisor = max(1, div(cardinality, max(1, idx + 1) * 5))
      {key, "#{key}_#{rem(div(i, divisor) + idx, max(2, cardinality))}"}
    end)
  end

  defp pick_value_for(%Metrics.Counter{}, _i), do: 1

  defp pick_value_for(%Metrics.Distribution{}, i) do
    # Walk values across the full default-bucket range so we land in different
    # buckets and drive find_bucket/2 representatively.
    Enum.at(
      [1, 7, 12, 30, 60, 90, 150, 350, 600, 900, 1500, 3500, 6000, 8500, 15000],
      rem(i, 15)
    )
  end

  defp pick_value_for(_other, i), do: rem(i, 1000) + 1
end
