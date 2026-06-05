defmodule OtelMetricExporter.PullProducer do
  @moduledoc """
  GenStage producer that drains metrics from `OtelMetricExporter.MetricStore`
  on demand. Designed to be plugged into a Broadway pipeline via
  `producer: [module: {OtelMetricExporter.PullProducer, metric_store_name: :my_store}]`,
  but works with any GenStage consumer.

  ## Design

  On each demand cycle the producer calls `MetricStore.prepare_to_collect/1` (one fast
  GenServer call) to obtain a collector closure, then invokes the closure with the
  current demand as the row limit. The closure runs entirely in the producer's process —
  MetricStore is never blocked by ETS work. Partial generations are tracked by storing
  the continuation closure in producer state; ETS cleanup is handled inside the closure.

  ## Tick scheduling

  The tick interval accounts for elapsed time: if processing a batch took longer than
  `pull_interval`, the next tick fires immediately (delay=0) rather than waiting the
  full interval again. This keeps drain rate stable regardless of downstream latency.

  A tick is only scheduled when there is outstanding consumer demand. If `collector` is
  stored but demand is zero the tick is skipped — the stored collector will be picked up
  naturally by the next `handle_demand` call, avoiding pushing events before consumers
  are ready.
  """
  use GenStage

  alias OtelMetricExporter.MetricStore

  @default_pull_interval 1000

  def start_link(opts) do
    GenStage.start_link(__MODULE__, opts, name: opts[:name])
  end

  @impl true
  def init(opts) do
    state = %{
      metric_store_name: Keyword.fetch!(opts, :metric_store_name),
      pull_interval: Keyword.get(opts, :pull_interval, @default_pull_interval),
      pending_demand: 0,
      tick_ref: nil,
      collector: nil,
      last_tick_at: System.monotonic_time(:millisecond)
    }

    {:producer, state}
  end

  @impl true
  def handle_demand(incoming, state) do
    state = %{state | pending_demand: state.pending_demand + incoming}
    pull_and_emit(state)
  end

  @impl true
  def handle_info(:tick, state) do
    state = %{state | tick_ref: nil, last_tick_at: System.monotonic_time(:millisecond)}
    pull_and_emit(state)
  end

  # No demand — respect backpressure. Rows stay in ETS until consumers ask for more.
  # Any stored collector will be picked up by the next handle_demand call.
  defp pull_and_emit(%{pending_demand: 0} = state), do: {:noreply, [], state}

  defp pull_and_emit(%{collector: nil} = state) do
    # Cheap atomic check before making a GenServer call — keeps MetricStore
    # idle (and eligible to hibernate) between rotate_and_trim cycles.
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

  defp do_collect(state, collector, acc \\ [])

  defp do_collect(%{pending_demand: demand} = state, nil, acc) when demand > 0 do
    {:noreply, flatten_acc(acc), schedule_tick(state)}
  end

  defp do_collect(state, nil, acc), do: {:noreply, flatten_acc(acc), state}

  defp do_collect(state, collector, acc) do
    case collector.(state.pending_demand) do
      {:ok, events, done_or_more} ->
        state =
          case done_or_more do
            :done -> %{state | collector: nil}
            {:more, next_coll} -> %{state | collector: next_coll}
          end

        count = length(events)
        emit_telemetry(count, state.metric_store_name)
        new_demand = max(state.pending_demand - count, 0)
        state = %{state | pending_demand: new_demand}

        # Schedule next tick only when there is remaining demand.
        # If collector has more rows but demand is zero, skip the tick —
        # the stored collector will be used when demand arrives via handle_demand.
        case state do
          %{pending_demand: d, collector: nil} when d > 0 ->
            {:ok, next_collector} = MetricStore.prepare_to_collect(state.metric_store_name)
            do_collect(state, next_collector, [events | acc])

          state ->
            {:noreply, flatten_acc([events | acc]), state}
        end
    end
  end

  # Flatten the accumulated list of event batches into a single list.
  # The single-batch case (overwhelmingly common) returns the list directly — O(1).
  # Multiple batches (rare: only when several generations drain in one demand cycle)
  # reverse then concat — O(k) on batch count + O(n) on total events.
  defp flatten_acc([]), do: []
  defp flatten_acc([single]), do: single
  defp flatten_acc(acc), do: acc |> Enum.reverse() |> Enum.concat()

  defp emit_telemetry(emitted, metric_store_name) do
    :telemetry.execute(
      [:otel_metric_exporter, :pull_producer, :pull],
      %{emitted: emitted, remaining: MetricStore.record_count(metric_store_name)},
      %{metric_store_name: metric_store_name}
    )
  end

  # Guard: don't double-schedule if a tick is already pending.
  defp schedule_tick(%{tick_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_tick(state) do
    # Account for time already elapsed since the last tick so the effective
    # drain interval stays close to pull_interval even when processing is slow.
    elapsed = System.monotonic_time(:millisecond) - state.last_tick_at
    delay = max(state.pull_interval - elapsed, 0)
    ref = Process.send_after(self(), :tick, delay)
    %{state | tick_ref: ref}
  end
end
