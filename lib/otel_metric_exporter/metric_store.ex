defmodule OtelMetricExporter.MetricStore do
  @moduledoc false

  use GenServer

  require Logger

  alias Telemetry.Metrics

  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.{
    Metric,
    NumberDataPoint,
    HistogramDataPoint,
    Sum,
    Gauge,
    Histogram
  }

  alias OtelMetricExporter.OtelApi

  import OtelMetricExporter.OtlpUtils, only: [build_kv: 1]

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  @current_gen_idx 1
  @earliest_gen_idx 2
  @max_counter_value 2 ** 64 - 1

  defmodule State do
    @moduledoc false
    defstruct [:config, :api, :metrics, :metrics_table, :generations_table]

    @type t :: %__MODULE__{
            config: map(),
            api: struct(),
            metrics: list(),
            metrics_table: atom(),
            generations_table: :ets.tid()
          }
  end

  @doc false
  def default_buckets, do: @default_buckets

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, genserver_opts(config))
  end

  defp genserver_opts(config) do
    [
      name: config.name,
      hibernate_after: config[:hibernate_after],
      spawn_opt: config[:spawn_opt]
    ]
    |> Enum.filter(fn {_, v} -> v end)
  end

  def get_metrics(metrics_table, generation \\ nil) do
    generation = generation || get_current_gen(metrics_table)

    :ets.match_object(metrics_table, {{generation, :_, :_, :_, :_}, :_, :_})
    |> group_rows()
  end

  defp group_rows(rows) do
    Enum.reduce(rows, %{}, fn
      {{_, name, :distribution, tags, bucket}, count, sum}, acc ->
        Map.update(
          acc,
          {:distribution, name},
          %{tags => %{bucket => {count, sum}}},
          fn all_tags ->
            Map.update(all_tags, tags, %{bucket => {count, sum}}, fn all_buckets ->
              Map.put(all_buckets, bucket, {count, sum})
            end)
          end
        )

      {{_, name, type, tags, _}, value, _}, acc ->
        Map.update(acc, {type, name}, %{tags => value}, fn all_tags ->
          Map.put(all_tags, tags, value)
        end)
    end)
  end

  @spec export_sync(term()) :: :ok | {:error, term()}
  def export_sync(name) do
    GenServer.call(name, :export_sync, :infinity)
  end

  def pull(name) do
    # Queue a rotation before collecting. Since GenServer processes messages in
    # order, the rotation completes before prepare_to_collect is handled.
    send(name, :rotate_and_trim)

    case prepare_to_collect(name) do
      {:ok, nil} ->
        {:ok, []}

      {:ok, collector} ->
        {:ok, metrics, :done} = collector.(:infinity)
        {:ok, metrics}
    end
  end

  @doc """
  Returns a resumable collector closure for the next available generation.

  The closure has type `(limit :: pos_integer | :infinity) -> {:ok, [Metric.t()], :done | {:more, collector}}`.
  Call it with a row limit to get a batch of metrics; repeat with the returned
  `{:more, next_collector}` until `:done`. ETS cleanup is handled inside the closure.
  """
  def prepare_to_collect(name), do: GenServer.call(name, :prepare_to_collect)

  @spec record_count(atom()) :: non_neg_integer()
  def record_count(name), do: :ets.info(name, :size)

  defp metric_type(%Metrics.Counter{}), do: :counter
  defp metric_type(%Metrics.Sum{}), do: :sum
  defp metric_type(%Metrics.LastValue{}), do: :last_value
  defp metric_type(%Metrics.Distribution{}), do: :distribution

  def write_metric(metrics_table, metric, value, tags),
    do: write_metric(metrics_table, metric, Enum.join(metric.name, "."), value, tags)

  def write_metric(metrics_table, %Metrics.Counter{} = metric, string_name, _, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}

    :ets.update_counter(metrics_table, ets_key, 1, {ets_key, 0, nil})
  end

  def write_metric(_metrics_table, %Metrics.Sum{} = _metric, _string_name, value, _tags)
      when not is_number(value), do: :ok

  def write_metric(metrics_table, %Metrics.Sum{} = metric, string_name, value, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}

    :ets.update_counter(metrics_table, ets_key, value, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.LastValue{} = metric, string_name, value, tags) do
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, nil}
    :ets.update_element(metrics_table, ets_key, {2, value}, {ets_key, value, nil})
  end

  def write_metric(_metrics_table, %Metrics.Distribution{} = _metric, _string_name, value, _tags)
      when not is_number(value), do: :ok

  def write_metric(metrics_table, %Metrics.Distribution{} = metric, string_name, value, tags) do
    bucket = find_bucket(metric, value)
    generation = get_current_gen(metrics_table)
    ets_key = {generation, string_name, metric_type(metric), tags, bucket}
    update_counter_op = {2, 1}
    update_sum_op = {3, round(value)}

    :ets.update_counter(
      metrics_table,
      ets_key,
      [update_counter_op, update_sum_op],
      {ets_key, 0, 0}
    )
  end

  defp find_bucket(%Metrics.Distribution{reporter_options: opts}, value) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)

    case Enum.find_index(bucket_bounds, &(value <= &1)) do
      # Overflow bucket
      nil -> length(bucket_bounds)
      idx -> idx
    end
  end

  @impl true
  def init(config) do
    metrics = Map.get(config, :metrics, [])
    metrics_table = config.name
    finch_pool = Map.get(config, :finch_pool, OtelMetricExporter.Finch)

    timer_message = if config[:pull_mode] == true, do: :rotate_and_trim, else: :export
    Process.send_after(self(), timer_message, config.export_period)

    # Create ETS table for metrics
    :ets.new(metrics_table, [:ordered_set, :public, :named_table, {:write_concurrency, :auto}])

    generations_table = :ets.new(:generations, [:ordered_set, :private])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    generation_counters = :atomics.new(2, signed: false)
    :persistent_term.put(generation_key(metrics_table), generation_counters)

    with {:ok, api, config} <- OtelApi.new(Map.put(config, :finch, finch_pool), :metrics) do
      {:ok,
       %State{
         config: config,
         api: api,
         metrics: metrics,
         metrics_table: metrics_table,
         generations_table: generations_table
       }}
    end
  end

  @impl true
  def handle_call(:export_sync, _from, state) do
    case export_metrics(state) do
      :ok ->
        {:reply, :ok, state}

      error ->
        {:reply, error, state}
    end
  end

  @impl true
  def handle_call(:prepare_to_collect, _from, state) do
    current_gen = get_current_gen(state.metrics_table)
    next_to_collect = peek_earliest_gen(state.metrics_table)

    if current_gen == next_to_collect do
      # No sealed generation available yet — rotate_and_trim has not fired.
      # Return an empty collector so the producer backs off until export_period elapses.
      {:reply, {:ok, nil}, state}
    else
      # Claim the next sealed generation and return a collector for it.
      # Use lookup (not pop) so the entry stays in generations_table until
      # the collector is fully drained — allowing trim_metrics_table to find
      # and clean up rows if the collector is abandoned under memory pressure.
      earliest_gen = bump_earliest_gen(state.metrics_table)
      gen_meta = lookup_generation(state, earliest_gen)
      collector = make_collector(gen_meta, state.metrics_table, state.config.name)
      {:reply, {:ok, collector}, state}
    end
  end

  @impl true
  def handle_cast({:release_generation, gen}, state) do
    :ets.delete(state.generations_table, gen)
    {:noreply, state}
  end

  @impl true
  def handle_info(:rotate_and_trim, state) do
    rotate_generation(state)
    Process.send_after(self(), :rotate_and_trim, state.config.export_period)
    {:noreply, state}
  end

  def handle_info(:export, state) do
    {duration, _} = :timer.tc(fn -> export_metrics(state) end, :millisecond)

    # schedule after we've sent to avoid problems when there's some kind of
    # problem sending and we get into a retry loop but take into account
    # time taken to send so we keep flushing the data on a regular interval
    Process.send_after(self(), :export, max(state.config.export_period - duration, 100))

    {:noreply, state}
  end

  defp rotate_generation(%State{} = state) do
    {old_gen, new_gen} = bump_gen_counter(state.metrics_table)

    :ets.update_element(
      state.generations_table,
      old_gen,
      {3, System.system_time(:nanosecond)}
    )

    :ets.insert(state.generations_table, {new_gen, System.system_time(:nanosecond), nil})

    trim_metrics_table(state)

    old_gen
  end

  defp trim_metrics_table(state) do
    if above_memory_limit?(state) do
      earliest_gen = earliest_gen(state.generations_table)
      current_gen = get_current_gen(state.metrics_table)
      previous_gen = current_gen - 1

      earliest_gen..previous_gen
      |> Enum.take_while(fn gen ->
        clear_generations(state, gen..gen)

        above_memory_limit?(state)
      end)
    end
  end

  defp above_memory_limit?(state) do
    memory_size = :ets.info(state.metrics_table, :memory) * :erlang.system_info(:wordsize)
    memory_size > state.api.config.max_table_memory
  end

  defp export_metrics(%State{} = state) do
    current_gen = rotate_generation(state)
    earliest_gen = earliest_gen(state.generations_table)

    metrics =
      collect_metrics(state, earliest_gen, current_gen) |> transform_metrics(state.metrics)

    case OtelApi.send_metrics(state.api, metrics) do
      :ok ->
        clear_generations(state, earliest_gen..current_gen//1)
        :ok

      {:error, reason} = err ->
        Logger.error("Failed to export metrics: #{inspect(reason)}")
        err
    end
  end

  defp collect_metrics(%State{} = state, earliest_gen, current_gen) do
    earliest_gen..current_gen//1
    |> Enum.flat_map(fn gen ->
      gen_meta = lookup_generation(state, gen)
      collect_metrics(state, gen_meta)
    end)
  end

  defp collect_metrics(state, {gen, start, finish}) do
    get_metrics(state.metrics_table, gen)
    |> Enum.map(fn {key, values} ->
      tagged_values = Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)
      {key, tagged_values}
    end)
  end

  defp transform_metrics(raw_metrics, metrics) when is_list(metrics) do
    raw_metrics
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {{type, name}, grouped_values} ->
      metric =
        Enum.find(metrics, &(Enum.join(&1.name, ".") == name and metric_type(&1) == type))

      convert_metric(metric, List.flatten(grouped_values))
    end)
  end

  defp make_collector({nil, nil, nil}, _table, _store), do: nil

  defp make_collector({gen, _, _} = gen_meta, table, store) do
    fn limit ->
      pattern = {{gen, :_, :_, :_, :_}, :_, :_}

      {objects, has_more} =
        case limit do
          :infinity ->
            objs = :ets.match_object(table, pattern)
            {objs, false}

          n when is_integer(n) and n > 0 ->
            case :ets.match_object(table, pattern, n) do
              :"$end_of_table" ->
                {[], false}

              {objs, _continuation} ->
                for obj <- objs, do: :ets.delete_object(table, obj)
                more = :ets.match_object(table, pattern, 1) != :"$end_of_table"
                {objs, more}
            end
        end

      if limit == :infinity do
        :ets.match_delete(table, pattern)
      end

      events = rows_to_events(objects, gen_meta)

      if has_more do
        {:ok, events, {:more, make_collector(gen_meta, table, store)}}
      else
        # All rows drained — release the generations_table entry via cast so
        # trim_metrics_table can no longer see it and the gen is fully cleaned up.
        GenServer.cast(store, {:release_generation, gen})
        {:ok, events, :done}
      end
    end
  end

  # Converts a batch of raw ETS rows to ingestion events.
  # Distribution metrics are not supported in pull mode — the collector does not
  # have access to the bucket bounds (stored in Metrics.Distribution reporter_options,
  # not in ETS) needed to reconstruct a well-formed histogram event.
  defp rows_to_events(rows, gen_meta) do
    {dist_rows, simple_rows} =
      Enum.split_with(rows, fn
        {{_, _, :distribution, _, _}, _, _} -> true
        _ -> false
      end)

    if dist_rows != [] do
      Logger.warning("Pull mode does not support distribution metrics — dropping rows")
    end

    Enum.map(simple_rows, &row_to_event(&1, gen_meta))
  end

  # Normalize tag values: the old protobuf path converted atoms to strings via encoding;
  # we replicate that here so downstream consumers see the same attribute types.
  defp normalize_tags(tags) when is_map(tags) do
    Map.new(tags, fn
      {k, v} when is_atom(v) -> {k, Atom.to_string(v)}
      {k, v} -> {k, v}
    end)
  end

  # metric_type strings must match what OtelMetric.handle_metric produced via the
  # old protobuf path so downstream BigQuery schemas stay consistent:
  #   :distribution → "histogram"  (was {:histogram, ...})
  #   :last_value   → "gauge"      (was {:gauge, ...})
  #   :counter/:sum → "sum"        (both became {:sum, ...})
  defp row_to_event({{_gen, name, :distribution, tags, bucket}, count, sum}, {_, start, finish}) do
    %{
      "event_message" => name,
      "metric_type" => "histogram",
      "metadata" => %{"type" => "metric"},
      "bucket" => bucket,
      "count" => count,
      "sum" => sum,
      "attributes" => normalize_tags(tags),
      "start_time" => start,
      "timestamp" => finish
    }
  end

  defp row_to_event({{_gen, name, :last_value, tags, _}, value, _}, {_, start, finish}) do
    %{
      "event_message" => name,
      "metric_type" => "gauge",
      "metadata" => %{"type" => "metric"},
      "value" => value,
      "attributes" => normalize_tags(tags),
      "start_time" => start,
      "timestamp" => finish
    }
  end

  @sum_temporality "cumulative"

  defp row_to_event({{_gen, name, type, tags, _}, value, _}, {_, start, finish})
       when type in [:counter, :sum] do
    %{
      "event_message" => name,
      "metric_type" => "sum",
      "metadata" => %{"type" => "metric"},
      "aggregation_temporality" => @sum_temporality,
      "is_monotonic" => type == :counter,
      "value" => value,
      "attributes" => normalize_tags(tags),
      "start_time" => start,
      "timestamp" => finish
    }
  end


  defp convert_metric(
         %{name: name, description: description, unit: unit} = metric,
         values
       ) do
    %Metric{
      name: Enum.join(name, "."),
      description: description,
      unit: convert_unit(unit),
      data: convert_data(metric, values)
    }
  end

  defp convert_data(%Metrics.Counter{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_int, value}
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: true
     }}
  end

  defp convert_data(%Metrics.Sum{}, values) do
    {:sum,
     %Sum{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_int, value}
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE,
       is_monotonic: false
     }}
  end

  defp convert_data(%Metrics.LastValue{}, values) do
    {:gauge,
     %Gauge{
       data_points:
         Enum.map(values, fn {{from, to}, tags, value} ->
           %NumberDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             value: {:as_double, value}
           }
         end)
     }}
  end

  defp convert_data(%Metrics.Distribution{reporter_options: opts}, values) do
    bucket_bounds = Keyword.get(opts, :buckets, @default_buckets)
    total_bucket_bounds = length(bucket_bounds)

    {:histogram,
     %Histogram{
       data_points:
         Enum.map(values, fn {{from, to}, tags, bucket_values} ->
           {total_count, total_sum} =
             Enum.reduce(bucket_values, {0, 0.0}, fn {_, {count, sum}},
                                                     {total_count, total_sum} ->
               {total_count + count, total_sum + sum}
             end)

           bucket_counts =
             Enum.map(0..total_bucket_bounds//1, &elem(Map.get(bucket_values, &1, {0, 0}), 0))

           %HistogramDataPoint{
             attributes: build_kv(tags),
             start_time_unix_nano: from,
             time_unix_nano: to,
             count: total_count,
             sum: total_sum,
             bucket_counts: bucket_counts,
             explicit_bounds: bucket_bounds
           }
         end),
       aggregation_temporality: :AGGREGATION_TEMPORALITY_CUMULATIVE
     }}
  end

  defp convert_unit(:unit), do: nil
  defp convert_unit(:second), do: "s"
  defp convert_unit(:millisecond), do: "ms"
  defp convert_unit(:microsecond), do: "us"
  defp convert_unit(:nanosecond), do: "ns"
  defp convert_unit(:byte), do: "By"
  defp convert_unit(:kilobyte), do: "kBy"
  defp convert_unit(:megabyte), do: "MBy"
  defp convert_unit(:gigabyte), do: "GBy"
  defp convert_unit(:terabyte), do: "TBy"
  defp convert_unit(x) when is_atom(x), do: Atom.to_string(x)

  defp lookup_generation(state, gen) do
    :ets.lookup(state.generations_table, gen)
    |> List.first({nil, nil, nil})
  end

  defp pop_generation(state, gen) do
    :ets.take(state.generations_table, gen)
    |> List.first({nil, nil, nil})
  end

  defp clear_generations(state, range) do
    for gen <- range do
      :ets.match_delete(state.metrics_table, {{gen, :_, :_, :_, :_}, :_, :_})
      :ets.delete(state.generations_table, gen)
    end
  end

  # TODO: Remove after migrating to pull-only
  defp earliest_gen(generations_table) do
    case :ets.first(generations_table) do
      :"$end_of_table" -> 0
      x -> x
    end
  end

  defp get_current_gen(table) do
    counters = :persistent_term.get(generation_key(table))
    :atomics.get(counters, @current_gen_idx)
  end

  defp bump_gen_counter(table) do
    counters = :persistent_term.get(generation_key(table))
    next_gen = :atomics.add_get(counters, @current_gen_idx, 1)
    old_gen = if next_gen != 0, do: next_gen - 1, else: @max_counter_value
    {old_gen, next_gen}
  end

  # Read without incrementing — safe because prepare_to_collect is a GenServer call
  # and earliest_gen is only modified from within this GenServer.
  defp peek_earliest_gen(table) do
    counters = :persistent_term.get(generation_key(table))
    :atomics.get(counters, @earliest_gen_idx)
  end

  defp bump_earliest_gen(table) do
    counters = :persistent_term.get(generation_key(table))
    next_gen = :atomics.add_get(counters, @earliest_gen_idx, 1)
    if next_gen != 0, do: next_gen - 1, else: @max_counter_value
  end

  defp generation_key(metrics_table) do
    {__MODULE__, metrics_table, :generation}
  end
end
