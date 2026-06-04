# Pull-mode benchmark using Broadway — matching the IngestPipeline architecture.
#
# PullProducer → Broadway processors → Broadway batchers → handle_batch (write sim)
# Samples ETS size, generation counts, and producer internals every 50ms.
#
# Run with:
#   mix run bench/pull_debug.exs

alias Telemetry.Metrics
alias OtelMetricExporter.MetricStore
alias OtelMetricExporter.PullProducer

Application.ensure_all_started(:broadway)
Process.flag(:trap_exit, true)

# ---- Knobs ----
num_writers          = 4
run_duration_ms      = 5_000
write_batch          = 50
pull_interval_ms     = 1000
export_period = 2000
# Mirror the previous GenStage config: max_demand=50_000, consumer_batch_size=50_000.
# 1 processor → 1 batcher means Broadway sends exactly batch_size demand to the producer.
# Batch fills immediately when 50k events arrive so batch_timeout rarely triggers.
batch_size           = 10_000
batch_timeout_ms     = 500
processor_concurrency = 4
batcher_concurrency   = 4
write_delay_ms       = 20         # ms per handle_batch call (simulates DB write latency)
# ---------------

metric     = Metrics.sum("bench.pull.throughput")
store_name = :bench_pull_debug

# Counters stored in persistent_term so Broadway callbacks can reach them
# without needing to thread state through Broadway's API.
dp_counter    = :atomics.new(1, signed: false)
cycle_counter = :atomics.new(1, signed: false)
peak_memory   = :atomics.new(1, signed: false)
unique_tag    = :atomics.new(1, signed: false)
stop_flag     = :atomics.new(1, signed: false)

:persistent_term.put(:pull_debug_counters, %{
  dp_counter:    dp_counter,
  cycle_counter: cycle_counter,
  write_delay_ms: write_delay_ms
})

defmodule PullDebugPipeline do
  use Broadway

  alias OtelMetricExporter.PullProducer

  def start_link(opts) do
    Broadway.start_link(__MODULE__,
      name: __MODULE__,
      producer: [
        module: {PullProducer,
                 metric_store_name: Keyword.fetch!(opts, :store_name),
                 pull_interval: Keyword.fetch!(opts, :pull_interval)},
        transformer: {__MODULE__, :transform, []},
        concurrency: 1
      ],
      processors: [
        default: [
          concurrency: Keyword.fetch!(opts, :processor_concurrency),
          max_demand: Keyword.fetch!(opts, :batch_size)
        ]
      ],
      batchers: [
        default: [
          concurrency: Keyword.fetch!(opts, :batcher_concurrency),
          batch_size: Keyword.fetch!(opts, :batch_size),
          batch_timeout: Keyword.fetch!(opts, :batch_timeout)
        ]
      ]
    )
  end

  def transform(event, _opts) do
    %Broadway.Message{data: event, acknowledger: {__MODULE__, :ack_id, :ack_data}}
  end

  def ack(_ack_ref, _successful, _failed), do: :ok

  @impl true
  def handle_message(_processor, message, _context), do: message

  @impl true
  def handle_batch(:default, messages, _batch_info, _context) do
    %{dp_counter: dp, cycle_counter: cc, write_delay_ms: delay} =
      :persistent_term.get(:pull_debug_counters)

    :atomics.add(dp, 1, length(messages))
    :atomics.add(cc, 1, 1)
    if delay > 0, do: Process.sleep(delay)
    messages
  end
end

IO.puts("")
IO.puts("=== pull_debug (Broadway) ===")
IO.puts("Writers: #{num_writers}, Duration: #{run_duration_ms}ms, write_batch: #{write_batch}")
IO.puts("pull_interval: #{pull_interval_ms}ms")
IO.puts("batch_size: #{batch_size}, batch_timeout: #{batch_timeout_ms}ms")
IO.puts("processor_concurrency: #{processor_concurrency}, batcher_concurrency: #{batcher_concurrency}")
IO.puts("write_delay_ms: #{write_delay_ms}")
IO.puts("")

{:ok, store_pid} =
  MetricStore.start_link(%{
    export_period: export_period,
    metrics: [metric],
    name: store_name,
    pull_mode: true
  })

{:ok, _pipeline_pid} =
  PullDebugPipeline.start_link(
    store_name: store_name,
    pull_interval: pull_interval_ms,
    batch_size: batch_size,
    batch_timeout: batch_timeout_ms,
    processor_concurrency: processor_concurrency,
    batcher_concurrency: batcher_concurrency
  )

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
             String.pad_leading("sealed_gens", 12) <>
             String.pad_leading("net_del", 10) <>
             String.pad_leading("emitted", 10) <>
             String.pad_leading("demand", 8) <>
             String.pad_leading("collector", 10) <>
             String.pad_leading("ets_kb", 8) <>
             String.pad_leading("proc_kb", 9)

    IO.puts(header)
    IO.puts(String.duplicate("-", String.length(header)))

    loop = fn loop, tick ->
      if :atomics.get(stop_flag, 1) == 0 do
        mem  = :ets.info(store_name, :memory) * :erlang.system_info(:wordsize)
        if mem > :atomics.get(peak_memory, 1), do: :atomics.put(peak_memory, 1, mem)

        rows    = :ets.info(store_name, :size)
        emitted = :atomics.get(dp_counter, 1)

        net_del = max(:atomics.get(prev_rows, 1) - rows, 0)
        :atomics.put(prev_rows, 1, rows)

        new_emitted = emitted - :atomics.get(prev_emitted, 1)
        :atomics.put(prev_emitted, 1, emitted)

        # Broadway wraps PullProducer — inspect its state via Broadway's API
        {demand, has_collector} =
          try do
            [prod | _] = Broadway.producer_names(PullDebugPipeline)
            s = :sys.get_state(Process.whereis(prod)).state.module_state
            {s.pending_demand, s.collector != nil}
          rescue
            _ -> {?-, false}
          end

        collector_str = if has_collector, do: "yes", else: "no"

        total_gens =
          try do
            counters = :persistent_term.get(gen_key)
            :atomics.get(counters, 1)
          rescue
            _ -> ?-
          end

        sealed_gens =
          try do
            caller = self()
            :sys.replace_state(store_pid, fn s ->
              send(caller, {:gen_count, :ets.info(s.generations_table, :size)})
              s
            end)
            receive do
              {:gen_count, n} -> n
            after
              50 -> ?-
            end
          rescue
            _ -> ?-
          end

        proc_kb = Float.round(:erlang.memory(:processes) / 1024, 0)

        IO.puts(
          String.pad_trailing("#{tick * 50}", 8) <>
          String.pad_leading("#{rows}", 10) <>
          String.pad_leading("#{total_gens}", 11) <>
          String.pad_leading("#{sealed_gens}", 12) <>
          String.pad_leading("#{net_del}", 10) <>
          String.pad_leading("#{new_emitted}", 10) <>
          String.pad_leading("#{demand}", 8) <>
          String.pad_leading("#{collector_str}", 10) <>
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

Supervisor.stop(PullDebugPipeline)
if Process.alive?(store_pid), do: GenServer.stop(store_pid, :shutdown, 2_000)
:persistent_term.erase(:pull_debug_counters)

IO.puts("")
IO.puts("=== Result ===")
IO.puts("  Writes issued     : #{unique_written}")
IO.puts("  Datapoints emitted: #{total_dp}")
IO.puts("  Batch cycles      : #{cycles}")
IO.puts("  DP / batch (avg)  : #{avg_per_cycle}")
IO.puts("  Throughput        : #{dp_per_s} dp/sec")
IO.puts("  Peak ETS memory   : #{peak_kb} KB")
IO.puts("  Writes - emitted  : #{unique_written - total_dp} (lag at stop)")
IO.puts("")
