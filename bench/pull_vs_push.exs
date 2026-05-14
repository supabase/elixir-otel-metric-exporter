# Compares pull-mode vs push-mode under a realistic high-cardinality workload
# with two consumer profiles (fast and slow).
#
# Each write produces a UNIQUE ETS row (monotonic counter as a tag), so per-cycle
# drain payload scales with elapsed time between drains. This is what surfaces:
#   - drain payload size (push has larger bursts at any cycle gap)
#   - peak ETS memory between drains
#   - throughput in *datapoints*, not Metric structs
#   - whether push's timer keeps up when the consumer is slow
#
# Run with:
#   mix run bench/pull_vs_push_realistic.exs

alias Telemetry.Metrics
alias OtelMetricExporter.MetricStore
alias OtelMetricExporter.PullProducer

Application.ensure_all_started(:gen_stage)
Process.flag(:trap_exit, true)

# ---- Knobs ----
num_writers = 4
run_duration_ms = 2_000
write_batch = 50
pull_interval_ms = 20
export_period_ms = 20
slow_consumer_delay_ms = 30
# ---------------

metric = Metrics.sum("bench.realistic.throughput")

defmodule RealisticConsumer do
  use GenStage

  def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

  @impl true
  def init(opts) do
    {:consumer,
     %{
       dp_counter: Keyword.fetch!(opts, :dp_counter),
       cycle_counter: Keyword.fetch!(opts, :cycle_counter),
       delay_ms: Keyword.fetch!(opts, :delay_ms)
     }}
  end

  @impl true
  def handle_events(events, _from, state) do
    dp_count =
      Enum.reduce(events, 0, fn %{data: {_kind, %{data_points: dps}}}, acc ->
        acc + length(dps)
      end)

    :atomics.add(state.dp_counter, 1, dp_count)
    :atomics.add(state.cycle_counter, 1, 1)
    if state.delay_ms > 0, do: Process.sleep(state.delay_ms)

    {:noreply, [], state}
  end
end

run_scenario = fn label, setup_fn ->
  IO.puts(">>> starting #{label}")
  dp_counter = :atomics.new(1, signed: false)
  cycle_counter = :atomics.new(1, signed: false)
  peak_memory = :atomics.new(1, signed: false)
  unique_tag = :atomics.new(1, signed: false)

  {pids, extras} =
    try do
      setup_fn.(dp_counter, cycle_counter)
    rescue
      e ->
        IO.puts("!!! setup failed: #{Exception.message(e)}")
        IO.puts(Exception.format(:error, e, __STACKTRACE__))
        reraise e, __STACKTRACE__
    end

  IO.puts(">>> setup ok for #{label}")

  stop_flag = :atomics.new(1, signed: false)

  writers =
    for w <- 1..num_writers do
      Task.async(fn ->
        loop = fn loop ->
          if :atomics.get(stop_flag, 1) == 0 do
            for _ <- 1..write_batch do
              slot = :atomics.add_get(unique_tag, 1, 1)

              MetricStore.write_metric(
                Keyword.fetch!(pids, :store_name),
                metric,
                1,
                %{writer: w, slot: slot}
              )
            end

            loop.(loop)
          end
        end

        loop.(loop)
      end)
    end

  sampler =
    Task.async(fn ->
      loop = fn loop ->
        if :atomics.get(stop_flag, 1) == 0 do
          mem =
            :ets.info(Keyword.fetch!(pids, :store_name), :memory) *
              :erlang.system_info(:wordsize)

          if mem > :atomics.get(peak_memory, 1), do: :atomics.put(peak_memory, 1, mem)
          Process.sleep(25)
          loop.(loop)
        end
      end

      loop.(loop)
    end)

  t0 = System.monotonic_time(:millisecond)
  Process.sleep(run_duration_ms)
  :atomics.put(stop_flag, 1, 1)

  if Keyword.get(extras, :push_mode, false), do: Process.sleep(500)

  t1 = System.monotonic_time(:millisecond)
  elapsed_s = (t1 - t0) / 1000.0

  Task.await_many(writers, 10_000)
  Task.await(sampler, 2_000)

  total_dp = :atomics.get(dp_counter, 1)
  cycles = :atomics.get(cycle_counter, 1)
  peak_kb = Float.round(:atomics.get(peak_memory, 1) / 1024, 1)
  dp_per_s = Float.round(total_dp / elapsed_s, 1)
  avg_per_cycle = if cycles > 0, do: Float.round(total_dp / cycles, 1), else: 0.0
  unique_written = :atomics.get(unique_tag, 1)

  for {_, v} <- pids, is_pid(v), Process.alive?(v) do
    GenServer.stop(v, :shutdown, 2_000)
  end

  IO.puts("  #{label}:")
  IO.puts("    Writes issued     : #{unique_written}")
  IO.puts("    Datapoints emitted: #{total_dp}")
  IO.puts("    Drain cycles      : #{cycles}")
  IO.puts("    DP / cycle (avg)  : #{avg_per_cycle}")
  IO.puts("    Throughput        : #{dp_per_s} dp/sec")
  IO.puts("    Peak ETS memory   : #{peak_kb} KB")
  IO.puts("    Writes - emitted  : #{unique_written - total_dp} (lag at stop)")
  IO.puts("")

  %{
    label: label,
    written: unique_written,
    emitted: total_dp,
    cycles: cycles,
    dp_per_cycle: avg_per_cycle,
    dp_per_s: dp_per_s,
    peak_kb: peak_kb
  }
end

pull_setup = fn delay_ms ->
  fn dp_counter, cycle_counter ->
    store_name = :"bench_realistic_pull_#{delay_ms}"

    {:ok, store_pid} =
      MetricStore.start_link(%{
        export_period: 60_000,
        metrics: [metric],
        name: store_name,
        pull_mode: true
      })

    {:ok, producer_pid} =
      GenStage.start_link(PullProducer,
        metric_store_name: store_name,
        pull_interval: pull_interval_ms
      )

    {:ok, consumer_pid} =
      GenStage.start_link(RealisticConsumer,
        dp_counter: dp_counter,
        cycle_counter: cycle_counter,
        delay_ms: delay_ms
      )

    GenStage.sync_subscribe(consumer_pid,
      to: producer_pid,
      max_demand: 1000,
      cancel: :temporary
    )

    pids = [
      store_name: store_name,
      producer: producer_pid,
      consumer: consumer_pid,
      store_pid: store_pid
    ]

    {pids, []}
  end
end

push_setup = fn delay_ms ->
  fn dp_counter, cycle_counter ->
    store_name = :"bench_realistic_push_#{delay_ms}"

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
          if delay_ms > 0, do: Process.sleep(delay_ms)
          :ok
        end
      })

    {[store_name: store_name, store_pid: store_pid], [push_mode: true]}
  end
end

IO.puts("")
IO.puts("=== pull_vs_push ===")

IO.puts(
  "Writers: #{num_writers}, Duration: #{run_duration_ms}ms, Batch: #{write_batch}, " <>
    "pull_interval: #{pull_interval_ms}ms, export_period: #{export_period_ms}ms, " <>
    "slow_delay: #{slow_consumer_delay_ms}ms"
)

IO.puts("")

results = [
  run_scenario.("pull / fast consumer (0ms)", pull_setup.(0)),
  run_scenario.("push / fast callback (0ms)", push_setup.(0)),
  run_scenario.(
    "pull / slow consumer (#{slow_consumer_delay_ms}ms)",
    pull_setup.(slow_consumer_delay_ms)
  ),
  run_scenario.(
    "push / slow callback  (#{slow_consumer_delay_ms}ms)",
    push_setup.(slow_consumer_delay_ms)
  )
]

IO.puts("=== Summary ===")
IO.puts("")

header =
  String.pad_trailing("Scenario", 45) <>
    String.pad_leading("Writes", 12) <>
    String.pad_leading("Emitted", 12) <>
    String.pad_leading("Cycles", 10) <>
    String.pad_leading("DP/cyc", 10) <>
    String.pad_leading("DP/s", 12) <>
    String.pad_leading("PeakKB", 10)

IO.puts(header)
IO.puts(String.duplicate("-", String.length(header)))

for r <- results do
  IO.puts(
    String.pad_trailing(r.label, 45) <>
      String.pad_leading("#{r.written}", 12) <>
      String.pad_leading("#{r.emitted}", 12) <>
      String.pad_leading("#{r.cycles}", 10) <>
      String.pad_leading("#{r.dp_per_cycle}", 10) <>
      String.pad_leading("#{r.dp_per_s}", 12) <>
      String.pad_leading("#{r.peak_kb}", 10)
  )
end

IO.puts("")
