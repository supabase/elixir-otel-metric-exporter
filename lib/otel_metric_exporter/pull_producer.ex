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
      collector: nil
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
    state = %{state | tick_ref: nil}
    pull_and_emit(state)
  end

  defp pull_and_emit(%{pending_demand: 0} = state), do: {:noreply, [], state}

  defp pull_and_emit(%{collector: nil} = state) do
    {:ok, collector} = MetricStore.prepare_to_collect(state.metric_store_name)
    do_collect(state, collector)
  end

  defp pull_and_emit(%{collector: collector} = state) do
    do_collect(state, collector)
  end

  defp do_collect(state, collector) do
    case collector.(state.pending_demand) do
      {:ok, [], :done} ->
        emit_telemetry(0, state.metric_store_name)
        {:noreply, [], schedule_tick(%{state | collector: nil})}

      {:ok, metrics, done_or_more} ->
        state =
          case done_or_more do
            :done -> %{state | collector: nil}
            {:more, next_collector} -> %{state | collector: next_collector}
          end

        data_points_count = Enum.sum_by(metrics, &count_data_points/1)
        emit_telemetry(data_points_count, state.metric_store_name)
        new_demand = max(state.pending_demand - data_points_count, 0)
        state = %{state | pending_demand: new_demand}
        state = if new_demand > 0, do: schedule_tick(state), else: state
        {:noreply, metrics, state}
    end
  end

  defp emit_telemetry(emitted, metric_store_name) do
    :telemetry.execute(
      [:otel_metric_exporter, :pull_producer, :pull],
      %{emitted: emitted, remaining: MetricStore.record_count(metric_store_name)},
      %{metric_store_name: metric_store_name}
    )
  end

  defp count_data_points(%_metric{data: {_type, %_metric_type{data_points: points}}}),
    do: length(points)

  defp schedule_tick(%{tick_ref: ref} = state) when is_reference(ref), do: state

  defp schedule_tick(state) do
    ref = Process.send_after(self(), :tick, state.pull_interval)
    %{state | tick_ref: ref}
  end
end
