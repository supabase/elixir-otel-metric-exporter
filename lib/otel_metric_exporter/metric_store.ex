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

  defmodule State do
    @moduledoc false
    defstruct [
      :config,
      :api,
      :metrics,
      :metric_index,
      :metrics_table,
      :last_export,
      :generations_table
    ]

    @type t :: %__MODULE__{
            config: map(),
            api: struct(),
            metrics: list(),
            metric_index: map(),
            metrics_table: atom(),
            generations_table: :ets.tid(),
            last_export: nil | DateTime.t()
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
    generation = generation || :persistent_term.get(generation_key(metrics_table))

    metrics_table
    |> storage_tables()
    |> Enum.flat_map(&:ets.match_object(&1, {{generation, :_, :_, :_}, :_, :_}))
    |> Enum.reduce(%{}, fn
      {{_, ref, tags, _}, {:distribution_atomics, bucket_count, buckets, sum}, _}, acc ->
        {type, name, _metric} = resolve_metric_ref(metrics_table, ref)

        bucket_values =
          0..(bucket_count - 1)//1
          |> Map.new(fn bucket -> {bucket, {:atomics.get(buckets, bucket + 1), 0}} end)
          |> Map.put(:sum, :atomics.get(sum, 1))

        Map.update(
          acc,
          {type, name},
          %{tags => bucket_values},
          &Map.put(&1, tags, bucket_values)
        )

      {{_, ref, tags, bucket}, value, sum}, acc ->
        {type, name, _metric} = resolve_metric_ref(metrics_table, ref)

        case type do
          :distribution ->
            Map.update(
              acc,
              {:distribution, name},
              %{tags => %{bucket => {value, sum}}},
              fn all_tags ->
                Map.update(all_tags, tags, %{bucket => {value, sum}}, fn all_buckets ->
                  Map.put(all_buckets, bucket, {value, sum})
                end)
              end
            )

          _ ->
            Map.update(acc, {type, name}, %{tags => value}, fn all_tags ->
              Map.put(all_tags, tags, value)
            end)
        end
    end)
  end

  def export_sync(name) do
    GenServer.call(name, :export_sync, :infinity)
  end

  defp metric_type(%Metrics.Counter{}), do: :counter
  defp metric_type(%Metrics.Sum{}), do: :sum
  defp metric_type(%Metrics.LastValue{}), do: :last_value
  defp metric_type(%Metrics.Distribution{}), do: :distribution

  def write_metric(metrics_table, metric, value, tags),
    do: write_metric(metrics_table, metric, Enum.join(metric.name, "."), value, tags)

  def write_metric(metrics_table, %Metrics.Counter{} = metric, name_or_id, _, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, metric_ref(metrics_table, metric, name_or_id), tags, nil}

    metrics_table
    |> write_table()
    |> :ets.update_counter(ets_key, 1, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.Sum{} = metric, name_or_id, value, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, metric_ref(metrics_table, metric, name_or_id), tags, nil}

    metrics_table
    |> write_table()
    |> :ets.update_counter(ets_key, value, {ets_key, 0, nil})
  end

  def write_metric(metrics_table, %Metrics.LastValue{} = metric, name_or_id, value, tags) do
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, metric_ref(metrics_table, metric, name_or_id), tags, nil}
    :ets.update_element(write_table(metrics_table), ets_key, {2, value}, {ets_key, value, nil})
  end

  def write_metric(metrics_table, %Metrics.Distribution{} = metric, name_or_id, value, tags) do
    if distribution_storage(metrics_table) == :atomics do
      write_distribution_atomics(metrics_table, metric, name_or_id, value, tags)
    else
      write_distribution_ets(metrics_table, metric, name_or_id, value, tags)
    end
  end

  defp write_distribution_ets(
         metrics_table,
         %Metrics.Distribution{} = metric,
         name_or_id,
         value,
         tags
       ) do
    bucket = find_bucket(metric, value)
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, metric_ref(metrics_table, metric, name_or_id), tags, bucket}
    update_counter_op = {2, 1}
    update_sum_op = {3, round(value)}

    :ets.update_counter(
      write_table(metrics_table),
      ets_key,
      [update_counter_op, update_sum_op],
      {ets_key, 0, 0}
    )
  end

  defp write_distribution_atomics(
         metrics_table,
         %Metrics.Distribution{} = metric,
         name_or_id,
         value,
         tags
       ) do
    bucket = find_bucket(metric, value)
    generation = :persistent_term.get(generation_key(metrics_table))
    ets_key = {generation, metric_ref(metrics_table, metric, name_or_id), tags, nil}
    table = write_table(metrics_table)

    {:distribution_atomics, _bucket_count, buckets, sum} =
      case :ets.lookup(table, ets_key) do
        [{^ets_key, atomics, nil}] ->
          atomics

        [] ->
          atomics = new_distribution_atomics(metric)

          case :ets.insert_new(table, {ets_key, atomics, nil}) do
            true ->
              atomics

            false ->
              [{^ets_key, atomics, nil}] = :ets.lookup(table, ets_key)
              atomics
          end
      end

    :atomics.add(buckets, bucket + 1, 1)
    :atomics.add(sum, 1, round(value))
  end

  defp new_distribution_atomics(%Metrics.Distribution{reporter_options: opts}) do
    bucket_count = Keyword.get(opts, :buckets, @default_buckets) |> length() |> Kernel.+(1)

    {:distribution_atomics, bucket_count, :atomics.new(bucket_count, signed: false),
     :atomics.new(1, signed: true)}
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
    metric_index = build_metric_index(metrics)
    finch_pool = Map.get(config, :finch_pool, OtelMetricExporter.Finch)
    Process.send_after(self(), :export, config.export_period)

    storage = init_storage(metrics_table, config[:storage] || :ets)

    generations_table = :ets.new(:generations, [:ordered_set, :private])
    :ets.insert(generations_table, {0, System.system_time(:nanosecond), 0})
    :persistent_term.put(generation_key(metrics_table), 0)
    :persistent_term.put(storage_key(metrics_table), storage)
    :persistent_term.put(metric_refs_key(metrics_table), metric_index.refs)
    :persistent_term.put(metric_ids_key(metrics_table), metric_index.ids)

    :persistent_term.put(
      distribution_storage_key(metrics_table),
      config[:distribution_storage] || :ets
    )

    with {:ok, api, config} <- OtelApi.new(Map.put(config, :finch, finch_pool), :metrics) do
      {:ok,
       %State{
         config: config,
         api: api,
         metrics: metrics,
         metric_index: metric_index.refs,
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
  def handle_info(:export, state) do
    {duration, _} = :timer.tc(fn -> export_metrics(state) end, :millisecond)

    # schedule after we've sent to avoid problems when there's some kind of
    # problem sending and we get into a retry loop but take into account
    # time taken to send so we keep flushing the data on a regular interval
    Process.send_after(self(), :export, max(state.config.export_period - duration, 100))

    {:noreply, state}
  end

  defp rotate_generation(%State{} = state) do
    current_gen = :persistent_term.get(generation_key(state.metrics_table))
    :persistent_term.put(generation_key(state.metrics_table), current_gen + 1)

    :ets.update_element(
      state.generations_table,
      current_gen,
      {3, System.system_time(:nanosecond)}
    )

    :ets.insert(state.generations_table, {current_gen + 1, System.system_time(:nanosecond), nil})

    trim_metrics_table(state)

    current_gen
  end

  defp trim_metrics_table(state) do
    if above_memory_limit?(state) do
      earliest_gen = earliest_gen(state.generations_table)
      current_gen = :persistent_term.get(generation_key(state.metrics_table))
      previous_gen = current_gen - 1

      earliest_gen..previous_gen
      |> Enum.take_while(fn gen ->
        clear_generations(state, gen..gen)

        above_memory_limit?(state)
      end)
    end
  end

  defp above_memory_limit?(state) do
    memory_size =
      state.metrics_table
      |> storage_tables()
      |> Enum.reduce(0, fn table, acc -> acc + :ets.info(table, :memory) end)
      |> Kernel.*(:erlang.system_info(:wordsize))

    memory_size > state.api.config.max_table_memory
  end

  defp export_metrics(%State{} = state) do
    current_gen = rotate_generation(state)
    earliest_gen = earliest_gen(state.generations_table)
    metrics = collect_metrics(state, earliest_gen, current_gen)

    case OtelApi.send_metrics(state.api, metrics) do
      :ok ->
        if metrics != [], do: clear_generations(state, earliest_gen..current_gen//1)
        :ok

      {:error, reason} = err ->
        Logger.error("Failed to export metrics: #{inspect(reason)}")
        err
    end
  end

  defp collect_metrics(%State{} = state, earliest_gen, current_gen) do
    earliest_gen..current_gen//1
    |> Enum.flat_map(fn gen ->
      {_, start, finish} =
        List.first(:ets.lookup(state.generations_table, gen), {nil, nil, nil})

      get_metrics(state.metrics_table, gen)
      |> Enum.map(fn {key, values} ->
        tagged_values = Enum.map(values, fn {tags, value} -> {{start, finish}, tags, value} end)
        {key, tagged_values}
      end)
    end)
    |> Enum.group_by(&elem(&1, 0), &elem(&1, 1))
    |> Enum.map(fn {{type, name}, grouped_values} ->
      metric =
        Enum.find(state.metrics, &(Enum.join(&1.name, ".") == name and metric_type(&1) == type))

      convert_metric(metric, List.flatten(grouped_values))
    end)
  end

  defp earliest_gen(generations_table) do
    case :ets.first(generations_table) do
      :"$end_of_table" -> 0
      x -> x
    end
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
           total_sum = Map.get(bucket_values, :sum, :not_set)

           {total_count, summed_buckets} =
             Enum.reduce(bucket_values, {0, 0.0}, fn
               {:sum, _}, acc ->
                 acc

               {_, {count, sum}}, {total_count, total_sum} ->
                 {total_count + count, total_sum + sum}
             end)

           total_sum = if total_sum == :not_set, do: summed_buckets, else: total_sum

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

  defp clear_generations(state, range) do
    for gen <- range do
      for table <- storage_tables(state.metrics_table) do
        :ets.match_delete(table, {{gen, :_, :_, :_}, :_, :_})
      end

      :ets.delete(state.generations_table, gen)
    end
  end

  defp init_storage(metrics_table, :ets) do
    opts = [
      :ordered_set,
      :public,
      :named_table,
      {:write_concurrency, :auto},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ]

    :ets.new(metrics_table, opts)
    %{type: :ets, tables: metrics_table}
  end

  defp init_storage(_metrics_table, :striped) do
    opts = [
      :ordered_set,
      :public,
      {:write_concurrency, :auto},
      {:read_concurrency, true},
      {:decentralized_counters, true}
    ]

    tables =
      1..:erlang.system_info(:schedulers_online)
      |> Enum.map(fn _ -> :ets.new(__MODULE__, opts) end)
      |> List.to_tuple()

    %{type: :striped, tables: tables}
  end

  defp write_table(metrics_table) do
    case storage(metrics_table) do
      %{type: :ets, tables: table} ->
        table

      %{type: :striped, tables: tables} ->
        elem(tables, :erlang.system_info(:scheduler_id) - 1)
    end
  end

  defp storage_tables(metrics_table) do
    case storage(metrics_table) do
      %{type: :ets, tables: table} -> [table]
      %{type: :striped, tables: tables} -> Tuple.to_list(tables)
    end
  end

  defp storage(metrics_table) do
    :persistent_term.get(storage_key(metrics_table), %{type: :ets, tables: metrics_table})
  end

  defp build_metric_index(metrics) do
    metrics
    |> Enum.with_index()
    |> Enum.reduce(%{ids: %{}, refs: %{}}, fn {metric, id}, acc ->
      name = Enum.join(metric.name, ".")
      type = metric_type(metric)

      %{
        ids: Map.put(acc.ids, {type, name}, id),
        refs: Map.put(acc.refs, id, {type, name, metric})
      }
    end)
  end

  defp metric_ref(_metrics_table, _metric, id) when is_integer(id), do: id

  defp metric_ref(metrics_table, metric, string_name) do
    type = metric_type(metric)

    Map.get(
      :persistent_term.get(metric_ids_key(metrics_table), %{}),
      {type, string_name},
      {type, string_name}
    )
  end

  defp resolve_metric_ref(metrics_table, id) when is_integer(id) do
    Map.fetch!(:persistent_term.get(metric_refs_key(metrics_table), %{}), id)
  end

  defp resolve_metric_ref(_metrics_table, {type, name}) do
    {type, name, nil}
  end

  defp distribution_storage(metrics_table) do
    :persistent_term.get(distribution_storage_key(metrics_table), :ets)
  end

  defp generation_key(metrics_table) do
    {__MODULE__, metrics_table, :generation}
  end

  defp storage_key(metrics_table) do
    {__MODULE__, metrics_table, :storage}
  end

  defp metric_refs_key(metrics_table) do
    {__MODULE__, metrics_table, :metric_refs}
  end

  defp metric_ids_key(metrics_table) do
    {__MODULE__, metrics_table, :metric_ids}
  end

  defp distribution_storage_key(metrics_table) do
    {__MODULE__, metrics_table, :distribution_storage}
  end
end
