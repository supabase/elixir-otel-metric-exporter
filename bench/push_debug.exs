# Push-mode benchmark for debugging ETS memory and export behaviour.
# Mirrors pull_debug.exs so results are directly comparable.
#
# The export_callback receives protobuf Metric structs; data_points are
# counted the same way pull_debug counts flat-map events (one per ETS row).
#
# Run with:
#   mix run bench/push_debug.exs

alias Telemetry.Metrics
alias OtelMetricExporter.MetricStore

Process.flag(:trap_exit, true)

# ---- Knobs (keep in sync with pull_debug.exs for fair comparison) ----
num_writers     = 4
run_duration_ms = 5_000
write_batch     = 50
export_period_ms = 20     # how often MetricStore rotates and calls the callback
write_delay_ms  = 0       # ms per export callback (simulates DB write latency)
# -----------------------------------------------------------------------

metric = Metrics.sum("bench.push.throughput")
store_name = :bench_push_debug

IO.puts("")
IO.puts("=== push_debug ===")
IO.puts("Writers: #{num_writers}, Duration: #{run_duration_ms}ms, write_batch: #{write_batch}")
IO.puts("export_period: #{export_period_ms}ms, write_delay_ms: #{write_delay_ms}")
IO.puts("")

dp_counter    = :atomics.new(1, signed: false)
cycle_counter = :atomics.new(1, signed: false)
peak_memory   = :atomics.new(1, signed: false)
unique_tag    = :atomics.new(1, signed: false)
stop_flag     = :atomics.new(1, signed: false)

{:ok, store_pid} =
  MetricStore.start_link(%{
    export_period: export_period_ms,
    metrics: [metric],
    name: store_name,
    otlp_endpoint: "http://localhost:4318",
    export_callback: fn {:metrics, metrics}, _config ->
      dp_count =
        Enum.reduce(metrics, 0, fn %{data: {_kind, %{data_points: dps}}}, acc ->
          acc + length(dps)
        end)

      :atomics.add(dp_counter, 1, dp_count)
      :atomics.add(cycle_counter, 1, 1)
      if write_delay_ms > 0, do: Process.sleep(write_delay_ms)
      :ok
    end
  })

writers =
  for w <- 1..num_writers do
    Task.async(fn ->
      loop = fn loop ->
        if :atomics.get(stop_flag, 1) == 0 do
          for _ <- 1..write_batch do
            slot = :atomics.add_get(unique_tag, 1, 1)
            MetricStore.write_metric(store_name, metric, 1, %{writer: w, slot: slot})
          end
          loop.(loop)
        end
      end
      loop.(loop)
    end)
  end

sampler =
  Task.async(fn ->
    gen_key      = {OtelMetricExporter.MetricStore, store_name, :generation}
    prev_rows    = :atomics.new(1, signed: false)
    prev_emitted = :atomics.new(1, signed: false)

    header = String.pad_trailing("t(ms)", 8) <>
             String.pad_leading("rows", 10) <>
             String.pad_leading("total_gens", 11) <>
             String.pad_leading("net_del", 10) <>
             String.pad_leading("emitted", 10) <>
             String.pad_leading("ets_kb", 8) <>
             String.pad_leading("proc_kb", 9)

    IO.puts(header)
    IO.puts(String.duplicate("-", String.length(header)))

    loop = fn loop, tick ->
      if :atomics.get(stop_flag, 1) == 0 do
        mem = :ets.info(store_name, :memory) * :erlang.system_info(:wordsize)
        if mem > :atomics.get(peak_memory, 1), do: :atomics.put(peak_memory, 1, mem)

        rows    = :ets.info(store_name, :size)
        emitted = :atomics.get(dp_counter, 1)

        net_del = max(:atomics.get(prev_rows, 1) - rows, 0)
        :atomics.put(prev_rows, 1, rows)

        new_emitted = emitted - :atomics.get(prev_emitted, 1)
        :atomics.put(prev_emitted, 1, emitted)

        total_gens =
          try do
            counters = :persistent_term.get(gen_key)
            :atomics.get(counters, 1)
          rescue
            _ -> ?-
          end

        proc_kb = Float.round(:erlang.memory(:processes) / 1024, 0)

        IO.puts(
          String.pad_trailing("#{tick * 50}", 8) <>
          String.pad_leading("#{rows}", 10) <>
          String.pad_leading("#{total_gens}", 11) <>
          String.pad_leading("#{net_del}", 10) <>
          String.pad_leading("#{new_emitted}", 10) <>
          String.pad_leading("#{Float.round(mem / 1024, 0)}", 8) <>
          String.pad_leading("#{proc_kb}", 9)
        )

        Process.sleep(50)
        loop.(loop, tick + 1)
      end
    end

    loop.(loop, 0)
  end)

t0 = System.monotonic_time(:millisecond)
Process.sleep(run_duration_ms)
:atomics.put(stop_flag, 1, 1)
# Give the export callback time to flush the final generation
Process.sleep(export_period_ms * 3)

t1 = System.monotonic_time(:millisecond)
elapsed_s = (t1 - t0) / 1000.0

Task.await_many(writers, 10_000)
Task.await(sampler, 5_000)

total_dp       = :atomics.get(dp_counter, 1)
cycles         = :atomics.get(cycle_counter, 1)
peak_kb        = Float.round(:atomics.get(peak_memory, 1) / 1024, 1)
dp_per_s       = Float.round(total_dp / elapsed_s, 1)
avg_per_cycle  = if cycles > 0, do: Float.round(total_dp / cycles, 1), else: 0.0
unique_written = :atomics.get(unique_tag, 1)

if Process.alive?(store_pid), do: GenServer.stop(store_pid, :shutdown, 2_000)

IO.puts("")
IO.puts("=== Result ===")
IO.puts("  Writes issued     : #{unique_written}")
IO.puts("  Datapoints emitted: #{total_dp}")
IO.puts("  Export cycles     : #{cycles}")
IO.puts("  DP / cycle (avg)  : #{avg_per_cycle}")
IO.puts("  Throughput        : #{dp_per_s} dp/sec")
IO.puts("  Peak ETS memory   : #{peak_kb} KB")
IO.puts("  Writes - emitted  : #{unique_written - total_dp} (lag at stop)")
IO.puts("")
