defmodule OtelMetricExporter.PullProducer do
  @moduledoc """
  GenStage producer that drains metrics from `OtelMetricExporter.MetricStore`
  on demand. Designed to be plugged into a Broadway pipeline via
  `producer: [module: {OtelMetricExporter.PullProducer, metric_store_name: :my_store}]`,
  but works with any GenStage consumer.

  ## Design

  Collection is driven entirely by the tick. `handle_demand` only accumulates pending
  demand and schedules a tick — it never initiates a collection. This prevents the
  message queue from growing when many concurrent Broadway processors send demand
  simultaneously: each `handle_demand` returns immediately without blocking on a
  `MetricStore.prepare_to_collect/1` GenServer call.

  Each tick drains at most one generation: if the generation is fully drained and
  demand remains, a new tick is scheduled rather than immediately fetching the next
  generation. This keeps individual tick callbacks short and non-blocking.

  ## Tick scheduling

  The tick interval accounts for elapsed time: if processing a batch took longer than
  `pull_interval`, the next tick fires immediately (delay=0) rather than waiting the
  full interval again. This keeps drain rate stable regardless of downstream latency.

  A tick is only scheduled when there is outstanding consumer demand. If `collector` is
  stored but demand is zero the tick is skipped — the stored collector will be picked up
  on the next tick once demand arrives.
  """
  use GenStage

  alias OtelMetricExporter.MetricStore

  @default_pull_interval 1000
  @default_drain_tick_interval 100

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts) do
    state = %{
      metric_store_name: Keyword.fetch!(opts, :metric_store_name),
      pull_interval: Keyword.get(opts, :pull_interval, @default_pull_interval),
      drain_tick_interval: Keyword.get(opts, :drain_tick_interval, @default_drain_tick_interval),
      pending_demand: 0,
      tick_ref: nil,
      collector: nil,
      last_tick_at: System.monotonic_time(:millisecond)
    }

    {:producer, state}
  end

  @impl true
  def handle_demand(incoming, %{collector: nil} = state) do
    state = %{state | pending_demand: state.pending_demand + incoming}
    {:noreply, [], schedule_tick(state)}
  end

  def handle_demand(incoming, state) do
    state = %{state | pending_demand: state.pending_demand + incoming}
    {:noreply, [], schedule_drain_tick(state)}
  end

  @impl true
  def handle_info(:tick, state) do
    state = %{state | tick_ref: nil, last_tick_at: System.monotonic_time(:millisecond)}
    pull_and_emit(state)
  end

  defp pull_and_emit(%{pending_demand: 0} = state), do: {:noreply, [], state}

  defp pull_and_emit(%{collector: nil} = state) do
    if MetricStore.generation_available?(state.metric_store_name) do
      {:ok, collector} = MetricStore.prepare_to_collect(state.metric_store_name)
      do_collect(state, collector)
    else
      {:noreply, [], schedule_tick(state)}
    end
  end

  defp pull_and_emit(%{collector: collector} = state) do
    do_collect(state, collector)
  end

  defp do_collect(state, nil) do
    {:noreply, [], schedule_tick(state)}
  end

  defp do_collect(state, collector) do
    {:ok, events, done_or_more} = collector.(state.pending_demand)

    state =
      case done_or_more do
        :done -> %{state | collector: nil}
        {:more, next} -> %{state | collector: next}
      end

    count = length(events)
    emit_telemetry(count, state.metric_store_name)
    new_demand = max(state.pending_demand - count, 0)
    state = %{state | pending_demand: new_demand}

    # Generation drained with demand remaining: schedule a tick to pick up the
    # next generation rather than calling prepare_to_collect inline.
    if new_demand > 0 and state.collector == nil do
      {:noreply, events, schedule_tick(state)}
    else
      {:noreply, events, state}
    end
  end

  defp emit_telemetry(emitted, metric_store_name) do
    :telemetry.execute(
      [:otel_metric_exporter, :pull_producer, :pull],
      %{emitted: emitted, remaining: MetricStore.record_count(metric_store_name)},
      %{metric_store_name: metric_store_name}
    )
  end

  defp schedule_drain_tick(%{tick_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_drain_tick(state) do
    elapsed = System.monotonic_time(:millisecond) - state.last_tick_at
    delay = max(state.drain_tick_interval - elapsed, 0)
    ref = Process.send_after(self(), :tick, delay)
    %{state | tick_ref: ref}
  end

  defp schedule_tick(%{tick_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_tick(state) do
    elapsed = System.monotonic_time(:millisecond) - state.last_tick_at
    delay = max(state.pull_interval - elapsed, 0)
    ref = Process.send_after(self(), :tick, delay)
    %{state | tick_ref: ref}
  end
end
