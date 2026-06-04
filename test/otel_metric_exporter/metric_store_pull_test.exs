defmodule OtelMetricExporter.MetricStorePullTest do
  use ExUnit.Case, async: false

  alias Telemetry.Metrics
  alias OtelMetricExporter.MetricStore

  @name :metric_store_pull_test

  setup do
    config = %{
      export_period: 50,
      metrics: [],
      name: @name,
      pull_mode: true
    }

    {:ok, store_config: config}
  end

  # Trigger a rotation so prepare_to_collect has a sealed generation to drain.
  # In production this happens via rotate_and_trim; tests use this helper.
  defp rotate(name) do
    send(name, :rotate_and_trim)
    # Synchronise: a call to any GenServer function ensures the message was processed
    MetricStore.record_count(name)
    :ok
  end

  describe "prepare_to_collect — resumable collector closure" do
    setup %{store_config: config} do
      metric = Metrics.sum("pull.test.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "collector returns :done immediately when store is empty" do
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [], :done} = collector.(100)
    end

    test "collector returns one flat map per ETS row with :infinity limit" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 5, %{id: "a"})
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [event], :done} = collector.(:infinity)

      assert event["event_message"] == "pull.test.sum"
      assert event["metric_type"] == "sum"
      assert event["value"] == 5
      assert event["attributes"] == %{id: "a"}
      assert is_integer(event["start_time"])
      assert is_integer(event["timestamp"])
      assert event["aggregation_temporality"] == "cumulative"
      assert event["is_monotonic"] == false
      assert event["metadata"] == %{"type" => "metric"}
    end

    test "bounded drain: collector returns {:more, next} when rows remain" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})
      MetricStore.write_metric(@name, metric, 3, %{id: "c"})
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)

      # limit 1 — one row (one flat map) comes back, two remain
      assert {:ok, batch1, {:more, c2}} = collector.(1)
      assert length(batch1) == 1

      # limit 10 — drains the remaining two rows (two flat maps)
      assert {:ok, batch2, :done} = c2.(10)
      assert length(batch2) == 2

      # three rows written → three flat maps total
      assert length(batch1) + length(batch2) == 3
    end

    test "after :done, next prepare_to_collect starts a fresh generation" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 10, %{id: "x"})
      rotate(@name)
      {:ok, c1} = MetricStore.prepare_to_collect(@name)
      assert {:ok, _, :done} = c1.(:infinity)

      # Nothing new written — next rotation finds an empty generation
      rotate(@name)
      {:ok, c2} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [], :done} = c2.(:infinity)
    end

    test "writes after prepare_to_collect are isolated from the current collector" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)

      MetricStore.write_metric(@name, metric, 99, %{id: "new"})

      # Drain gen 0 row by row — "new" must not appear
      assert {:ok, batch1, {:more, c2}} = collector.(1)
      assert length(batch1) == 1

      assert {:ok, batch2, :done} = c2.(1)
      assert length(batch2) == 1

      # Rotate to seal gen 1, then collect the "new" write
      rotate(@name)
      {:ok, c3} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [event3], :done} = c3.(:infinity)

      assert event3["value"] == 99
    end

    test "metric_type mapping matches old OtelMetric.handle_metric output" do
      # Verify the type strings match what the old protobuf → handle_metric path produced
      # so BigQuery schemas and existing queries remain compatible.
      tags = %{id: "t"}

      counter  = Metrics.counter("m.counter")
      sum      = Metrics.sum("m.sum")
      gauge    = Metrics.last_value("m.gauge")

      MetricStore.write_metric(@name, counter, 1, tags)
      MetricStore.write_metric(@name, sum, 10, tags)
      MetricStore.write_metric(@name, gauge, 99, tags)
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)
      {:ok, events, :done} = collector.(:infinity)

      by_name = Map.new(events, fn e -> {e["event_message"], e} end)

      # Counter → "sum" (was {:sum, is_monotonic: true} in protobuf)
      assert by_name["m.counter"]["metric_type"] == "sum"
      assert by_name["m.counter"]["is_monotonic"] == true
      assert by_name["m.counter"]["aggregation_temporality"] == "cumulative"

      # Sum → "sum" (was {:sum, is_monotonic: false} in protobuf)
      assert by_name["m.sum"]["metric_type"] == "sum"
      assert by_name["m.sum"]["is_monotonic"] == false
      assert by_name["m.sum"]["aggregation_temporality"] == "cumulative"

      # LastValue → "gauge" (was {:gauge, ...} in protobuf)
      assert by_name["m.gauge"]["metric_type"] == "gauge"
      refute Map.has_key?(by_name["m.gauge"], "is_monotonic")
    end

    test "distribution rows are dropped with a warning in pull mode" do
      # Distribution metrics require bucket bounds (stored in Metrics.Distribution
      # reporter_options, not in ETS) to reconstruct a valid histogram event.
      # Pull mode drops them rather than emitting broken partial data.
      metric = Metrics.distribution("m.latency", reporter_options: [buckets: [10, 100, 1000]])
      sum    = Metrics.sum("m.bytes")

      MetricStore.write_metric(@name, metric, 50, %{"user_id" => "a"})
      MetricStore.write_metric(@name, sum, 100, %{"user_id" => "a"})
      rotate(@name)
      {:ok, collector} = MetricStore.prepare_to_collect(@name)

      import ExUnit.CaptureLog
      {events, log} = with_log(fn ->
        {:ok, evts, :done} = collector.(:infinity)
        evts
      end)

      assert length(events) == 1
      assert hd(events)["event_message"] == "m.bytes"
      assert log =~ "does not support distribution metrics"
      assert log =~ "m.latency"
    end

    test "full lifecycle across two generations" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 10, %{id: "g0"})
      rotate(@name)
      {:ok, c0} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [e0], :done} = c0.(:infinity)
      assert e0["value"] == 10

      MetricStore.write_metric(@name, metric, 20, %{id: "g1"})
      rotate(@name)
      {:ok, c1} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [e1], :done} = c1.(:infinity)
      assert e1["value"] == 20
    end
  end

  describe "pull/1 (unbounded, unchanged)" do
    setup %{store_config: config} do
      metric = Metrics.sum("pull.test.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "returns populated metrics and clears them" do
      metric = Metrics.sum("pull.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 5, tags)
      MetricStore.write_metric(@name, metric, 3, tags)

      assert {:ok, metrics} = MetricStore.pull(@name)
      assert length(metrics) == 1

      # Generation 0 should be cleared after pull
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "returns {:ok, []} on empty store" do
      assert {:ok, []} = MetricStore.pull(@name)
    end

    test "second sequential pull returns empty after first drained all metrics" do
      metric = Metrics.sum("pull.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 10, tags)

      assert {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1

      assert {:ok, []} = MetricStore.pull(@name)
    end

    test "concurrent writers during pull land in next generation" do
      metric = Metrics.sum("pull.test.sum")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)

      assert {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1

      MetricStore.write_metric(@name, metric, 2, tags)

      assert {:ok, second} = MetricStore.pull(@name)
      assert length(second) == 1
    end

    test "metrics written between two pulls are not lost or double-emitted" do
      metric = Metrics.sum("pull.test.sum")
      tags1 = %{test: "a"}
      tags2 = %{test: "b"}

      MetricStore.write_metric(@name, metric, 1, tags1)

      {:ok, first_batch} = MetricStore.pull(@name)
      assert length(first_batch) == 1

      MetricStore.write_metric(@name, metric, 2, tags2)

      {:ok, second_batch} = MetricStore.pull(@name)
      assert length(second_batch) == 1

      {:ok, third_batch} = MetricStore.pull(@name)
      assert third_batch == []
    end
  end

  test "max_table_memory enforced in pull mode without pull call" do
    metric = Telemetry.Metrics.sum("pull.trim.value")

    config = %{
      export_period: 60_000,
      metrics: [metric],
      name: @name,
      pull_mode: true,
      max_table_memory: 1
    }

    start_supervised!({MetricStore, config})

    MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})
    refute MetricStore.get_metrics(@name, 0) == %{}

    send(@name, :rotate_and_trim)
    :timer.sleep(50)
    MetricStore.write_metric(@name, metric, 1, %{"k" => "v"})

    send(@name, :rotate_and_trim)
    :timer.sleep(50)

    assert MetricStore.get_metrics(@name, 0) == %{}
    assert MetricStore.get_metrics(@name, 1) == %{}
  end

  describe "validation" do
    test "pull_mode: true with non-nil export_callback returns error" do
      config = %{
        export_period: 50,
        metrics: [],
        name: :pull_validation_test,
        pull_mode: true,
        export_callback: fn _payload, _config -> :ok end
      }

      assert {:error, _} = MetricStore.start_link(config)
    end
  end
end
