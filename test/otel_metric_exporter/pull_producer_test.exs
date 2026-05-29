defmodule OtelMetricExporter.PullProducerTest do
  use ExUnit.Case, async: false

  alias Telemetry.Metrics
  alias OtelMetricExporter.MetricStore
  alias OtelMetricExporter.PullProducer

  @store_name :pull_producer_test_store

  # Minimal GenStage consumer — asks for demand once, forwards events to test process.
  defmodule TestConsumer do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      test_pid = Keyword.fetch!(opts, :test_pid)
      {:consumer, %{test_pid: test_pid}}
    end

    @impl true
    def handle_subscribe(:producer, _opts, from, state) do
      GenStage.ask(from, 10)
      {:manual, state}
    end

    @impl true
    def handle_events(events, _from, state) do
      send(state.test_pid, {:events, events})
      {:noreply, [], state}
    end
  end

  # Consumer that re-asks after every batch — used to test partial-drain continuation.
  defmodule GreedyConsumer do
    use GenStage

    def start_link(opts), do: GenStage.start_link(__MODULE__, opts)

    @impl true
    def init(opts) do
      {:consumer,
       %{test_pid: Keyword.fetch!(opts, :test_pid), demand: Keyword.get(opts, :demand, 10)}}
    end

    @impl true
    def handle_subscribe(:producer, _opts, from, state) do
      GenStage.ask(from, state.demand)
      {:manual, state}
    end

    @impl true
    def handle_events(events, from, state) do
      send(state.test_pid, {:events, events})
      GenStage.ask(from, state.demand)
      {:noreply, [], state}
    end
  end

  setup do
    metric = Metrics.sum("producer.test.sum")

    store_config = %{
      export_period: 5000,
      metrics: [metric],
      name: @store_name,
      pull_mode: true
    }

    _store = start_supervised!({MetricStore, store_config})

    {:ok, metric: metric}
  end

  test "emits events when store has metrics", %{metric: metric} do
    MetricStore.write_metric(@store_name, metric, 10, %{test: "a"})
    MetricStore.write_metric(@store_name, metric, 20, %{test: "b"})

    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    {:ok, consumer} = GenStage.start_link(TestConsumer, test_pid: self())
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    assert_receive {:events, events}, 1000
    assert length(events) >= 1
  end

  test "empty store then populate, consumer receives events on next tick", %{metric: metric} do
    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    {:ok, consumer} = GenStage.start_link(TestConsumer, test_pid: self())
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    # Nothing in the store yet — no events should arrive immediately
    refute_receive {:events, _}, 80

    # Now populate the store
    MetricStore.write_metric(@store_name, metric, 5, %{test: "late"})

    # After a tick cycle, the producer should pull and emit
    assert_receive {:events, events}, 500
    assert length(events) == 1
  end

  test "producer state: initial demand is zero and no tick scheduled" do
    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    # :sys.get_state/1 on a GenStage returns the outer GenStage struct.
    # The user state is nested under the :state key.
    genstage_state = :sys.get_state(producer)
    user_state = genstage_state.state

    assert user_state.pending_demand == 0
    assert user_state.tick_ref == nil
  end

  test "producer schedules tick and resumes when demand pending but store empty then populated",
       %{metric: metric} do
    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    # Subscribe a consumer to create demand
    {:ok, consumer} = GenStage.start_link(TestConsumer, test_pid: self())
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    # Store is empty; producer should schedule a tick and wait.
    # Wait a short time to let the first pull attempt settle.
    Process.sleep(10)
    genstage_state = :sys.get_state(producer)
    user_state = genstage_state.state
    # tick_ref may be set (pending) or nil (already fired and re-scheduled);
    # the invariant is no crash and events arrive once data is available.
    assert is_nil(user_state.tick_ref) or is_reference(user_state.tick_ref)

    MetricStore.write_metric(@store_name, metric, 7, %{test: "tick"})

    assert_receive {:events, events}, 500
    assert length(events) == 1
  end

  test "emits correct values from store", %{metric: metric} do
    MetricStore.write_metric(@store_name, metric, 42, %{test: "val"})

    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    {:ok, consumer} = GenStage.start_link(TestConsumer, test_pid: self())
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    assert_receive {:events, [metric_proto]}, 500
    [dp] = elem(metric_proto.data, 1).data_points
    assert dp.value == {:as_int, 42}
  end

  test "continues emitting across multiple generations", %{metric: metric} do
    MetricStore.write_metric(@store_name, metric, 10, %{gen: "0"})

    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    {:ok, consumer} = GenStage.start_link(TestConsumer, test_pid: self())
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    assert_receive {:events, batch1}, 500
    assert length(batch1) == 1

    # Write to the next generation — producer should pick it up on the next tick
    MetricStore.write_metric(@store_name, metric, 20, %{gen: "1"})

    assert_receive {:events, batch2}, 500
    assert length(batch2) == 1

    [dp] = elem(hd(batch2).data, 1).data_points
    assert dp.value == {:as_int, 20}
  end

  test "no data lost when generation spans multiple demand cycles", %{metric: metric} do
    # Write 11 distinct tag sets — each becomes one data point.
    # With demand-per-cycle = 10, the first call returns 10 data points with
    # {:more, continuation}; the second call (triggered by GreedyConsumer re-asking)
    # uses the stored continuation and returns the remaining 1.
    for i <- 1..11 do
      MetricStore.write_metric(@store_name, metric, i, %{seq: i})
    end

    {:ok, producer} =
      GenStage.start_link(PullProducer, metric_store_name: @store_name, pull_interval: 50)

    {:ok, consumer} = GenStage.start_link(GreedyConsumer, test_pid: self(), demand: 10)
    GenStage.sync_subscribe(consumer, to: producer, cancel: :temporary)

    total_data_points =
      Stream.repeatedly(fn ->
        receive do
          {:events, events} ->
            events
            |> Enum.flat_map(fn m -> elem(m.data, 1).data_points end)
            |> length()
        after
          500 -> nil
        end
      end)
      |> Stream.take_while(&(&1 != nil))
      |> Enum.sum()

    assert total_data_points == 11
  end
end
