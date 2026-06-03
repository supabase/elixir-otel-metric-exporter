# Realistic bounded-cardinality benchmark.
#
# Unlike pull_debug / push_debug (which use a unique slot per write, creating
# unlimited ETS rows), this benchmark uses a FIXED set of user_ids so writes
# accumulate into the same ETS rows via update_counter — matching production:
#   num_users × num_metrics rows per generation, values grow, row count is capped.
#
# Varies rotation period across several values to show how rotation frequency
# affects: rows-per-generation, drain cycles, throughput, and peak ETS memory.
#
# Run with:
#   mix run bench/bounded_cardinality.exs

alias Telemetry.Metrics
alias OtelMetricExporter.MetricStore
alias OtelMetricExporter.PullProducer

Application.ensure_all_started(:gen_stage)
Process.flag(:trap_exit, true)

# ---- Knobs ----
num_writers      = 4
run_duration_ms  = 3_000
write_batch      = 50
num_users        = 1_000   # fixed tag cardinality: rows ≤ num_users per generation
max_demand       = 5_000   # must be ≥ num_users to drain a full generation per cycle

# Rotation periods to test (ms). For pull this is pull_interval_ms;
# for push this is export_period_ms. Shorter = more frequent snapshots.
rotation_periods = [20, 100, 500]
# ---------------

metrics = [
  Metrics.sum("bench.ingested_bytes"),
  Metrics.counter("bench.ingested_count", measurement: :count),
  Metrics.sum("bench.egress_bytes"),
  Metrics.sum("bench.query_bytes")
]

defmodule BoundedConsumer do
  use GenStage

  def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    {:consumer, %{
      dp_counter:    Keyword.fetch!(opts, :dp_counter),
      cycle_counter: Keyword.fetch!(opts, :cycle_counter)
    }}
  end

  @impl true
  def handle_events(events, _from, state) do
    :atomics.add(state.dp_counter, 1, length(events))
    :atomics.add(state.cycle_counter, 1, 1)
    {:noreply, [], state}
  end
end

run_pull = fn rotation_ms ->
  store_name = :"bounded_pull_#{rotation_ms}"

  dp_counter    = :atomics.new(1, signed: false)
  cycle_counter = :atomics.new(1, signed: false)
  peak_memory   = :atomics.new(1, signed: false)
  write_counter = :atomics.new(1, signed: false)
  stop_flag     = :atomics.new(1, signed: false)
  gen_key       = {OtelMetricExporter.MetricStore, store_name, :generation}

  {:ok, store_pid} =
    MetricStore.start_link(%{
      export_period: 60_000,
      metrics: metrics,
      name: store_name,
      pull_mode: true
    })

  {:ok, producer_pid} =
    GenStage.start_link(PullProducer,
      metric_store_name: store_name,
      pull_interval: rotation_ms
    )

  {:ok, consumer_pid} =
    GenStage.start_link(BoundedConsumer,
      dp_counter: dp_counter,
      cycle_counter: cycle_counter
    )

  GenStage.sync_subscribe(consumer_pid,
    to: producer_pid,
    max_demand: max_demand,
    cancel: :temporary
  )

  writers =
    for w <- 1..num_writers do
      Task.async(fn ->
        loop = fn loop, seq ->
          if :atomics.get(stop_flag, 1) == 0 do
            for i <- 1..write_batch do
              user_id = rem(seq * write_batch + i, num_users)
              MetricStore.write_metric(store_name, hd(metrics), 1_024, %{user_id: user_id})
            end
            :atomics.add(write_counter, 1, write_batch)
            loop.(loop, seq + 1)
          end
        end
        loop.(loop, w * 100_000)
      end)
    end

  sampler =
    Task.async(fn ->
      loop = fn loop ->
        if :atomics.get(stop_flag, 1) == 0 do
          mem = :ets.info(store_name, :memory) * :erlang.system_info(:wordsize)
          if mem > :atomics.get(peak_memory, 1), do: :atomics.put(peak_memory, 1, mem)
          Process.sleep(10)
          loop.(loop)
        end
      end
      loop.(loop)
    end)

  t0 = System.monotonic_time(:millisecond)
  Process.sleep(run_duration_ms)
  :atomics.put(stop_flag, 1, 1)

  Task.await_many(writers, 10_000)
  Task.await(sampler, 2_000)

  elapsed_s     = (System.monotonic_time(:millisecond) - t0) / 1000.0
  total_dp      = :atomics.get(dp_counter, 1)
  cycles        = :atomics.get(cycle_counter, 1)
  peak_kb       = Float.round(:atomics.get(peak_memory, 1) / 1024, 1)
  total_writes  = :atomics.get(write_counter, 1)
  total_gens    = try do
    counters = :persistent_term.get(gen_key)
    :atomics.get(counters, 1)
  rescue _ -> 0 end

  for pid <- [consumer_pid, producer_pid, store_pid], Process.alive?(pid) do
    GenServer.stop(pid, :shutdown, 2_000)
  end

  %{
    mode: :pull,
    rotation_ms: rotation_ms,
    writes: total_writes,
    emitted: total_dp,
    lag: total_writes - total_dp,
    drain_cycles: cycles,
    dp_per_cycle: (if cycles > 0, do: Float.round(total_dp / cycles, 0), else: 0),
    total_gens: total_gens,
    dp_per_s: Float.round(total_dp / elapsed_s, 0),
    peak_kb: peak_kb
  }
end

run_push = fn rotation_ms ->
  store_name = :"bounded_push_#{rotation_ms}"

  dp_counter    = :atomics.new(1, signed: false)
  cycle_counter = :atomics.new(1, signed: false)
  peak_memory   = :atomics.new(1, signed: false)
  write_counter = :atomics.new(1, signed: false)
  stop_flag     = :atomics.new(1, signed: false)
  gen_key       = {OtelMetricExporter.MetricStore, store_name, :generation}

  {:ok, store_pid} =
    MetricStore.start_link(%{
      export_period: rotation_ms,
      metrics: metrics,
      name: store_name,
      otlp_endpoint: "http://localhost:4318",
      export_callback: fn {:metrics, exported_metrics}, _config ->
        dp_count =
          Enum.reduce(exported_metrics, 0, fn %{data: {_, %{data_points: dps}}}, acc ->
            acc + length(dps)
          end)
        :atomics.add(dp_counter, 1, dp_count)
        :atomics.add(cycle_counter, 1, 1)
        :ok
      end
    })

  writers =
    for w <- 1..num_writers do
      Task.async(fn ->
        loop = fn loop, seq ->
          if :atomics.get(stop_flag, 1) == 0 do
            for i <- 1..write_batch do
              user_id = rem(seq * write_batch + i, num_users)
              MetricStore.write_metric(store_name, hd(metrics), 1_024, %{user_id: user_id})
            end
            :atomics.add(write_counter, 1, write_batch)
            loop.(loop, seq + 1)
          end
        end
        loop.(loop, w * 100_000)
      end)
    end

  sampler =
    Task.async(fn ->
      loop = fn loop ->
        if :atomics.get(stop_flag, 1) == 0 do
          mem = :ets.info(store_name, :memory) * :erlang.system_info(:wordsize)
          if mem > :atomics.get(peak_memory, 1), do: :atomics.put(peak_memory, 1, mem)
          Process.sleep(10)
          loop.(loop)
        end
      end
      loop.(loop)
    end)

  t0 = System.monotonic_time(:millisecond)
  Process.sleep(run_duration_ms)
  :atomics.put(stop_flag, 1, 1)
  Process.sleep(rotation_ms * 2)  # let final export flush

  Task.await_many(writers, 10_000)
  Task.await(sampler, 2_000)

  elapsed_s     = (System.monotonic_time(:millisecond) - t0) / 1000.0
  total_dp      = :atomics.get(dp_counter, 1)
  cycles        = :atomics.get(cycle_counter, 1)
  peak_kb       = Float.round(:atomics.get(peak_memory, 1) / 1024, 1)
  total_writes  = :atomics.get(write_counter, 1)
  total_gens    = try do
    counters = :persistent_term.get(gen_key)
    :atomics.get(counters, 1)
  rescue _ -> 0 end

  if Process.alive?(store_pid), do: GenServer.stop(store_pid, :shutdown, 2_000)

  %{
    mode: :push,
    rotation_ms: rotation_ms,
    writes: total_writes,
    emitted: total_dp,
    lag: total_writes - total_dp,
    drain_cycles: cycles,
    dp_per_cycle: (if cycles > 0, do: Float.round(total_dp / cycles, 0), else: 0),
    total_gens: total_gens,
    dp_per_s: Float.round(total_dp / elapsed_s, 0),
    peak_kb: peak_kb
  }
end

IO.puts("")
IO.puts("=== bounded_cardinality ===")
IO.puts("num_users: #{num_users}, num_metrics: #{length(metrics)}, max_rows_per_gen: #{num_users}")
IO.puts("Writers: #{num_writers}, Duration: #{run_duration_ms}ms, Batch: #{write_batch}, max_demand: #{max_demand}")
IO.puts("Rotation periods: #{inspect(rotation_periods)}ms")
IO.puts("")

results =
  for rotation_ms <- rotation_periods do
    IO.puts("--- pull  rotation=#{rotation_ms}ms ---")
    pull = run_pull.(rotation_ms)
    IO.puts("    writes=#{pull.writes} emitted=#{pull.emitted} lag=#{pull.lag} gens=#{pull.total_gens} peak=#{pull.peak_kb}KB dp/s=#{pull.dp_per_s}")

    IO.puts("--- push  rotation=#{rotation_ms}ms ---")
    push = run_push.(rotation_ms)
    IO.puts("    writes=#{push.writes} emitted=#{push.emitted} lag=#{push.lag} gens=#{push.total_gens} peak=#{push.peak_kb}KB dp/s=#{push.dp_per_s}")

    IO.puts("")
    [pull, push]
  end
  |> List.flatten()

IO.puts("=== Summary ===")
IO.puts("")

col = fn s, w -> String.pad_leading("#{s}", w) end
lft = fn s, w -> String.pad_trailing("#{s}", w) end

header =
  lft.("mode/rotation", 18) <>
  col.("writes", 10) <>
  col.("emitted", 10) <>
  col.("lag", 10) <>
  col.("gens", 7) <>
  col.("dp/gen", 9) <>
  col.("dp/s", 10) <>
  col.("peak_kb", 10)

IO.puts(header)
IO.puts(String.duplicate("-", String.length(header)))

for r <- results do
  IO.puts(
    lft.("#{r.mode}/#{r.rotation_ms}ms", 18) <>
    col.(r.writes, 10) <>
    col.(r.emitted, 10) <>
    col.(r.lag, 10) <>
    col.(r.total_gens, 7) <>
    col.(r.dp_per_cycle, 9) <>
    col.(r.dp_per_s, 10) <>
    col.(r.peak_kb, 10)
  )
end

IO.puts("")
