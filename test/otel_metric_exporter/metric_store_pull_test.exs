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

  describe "prepare_to_collect — resumable collector closure" do
    setup %{store_config: config} do
      metric = Metrics.sum("pull.test.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "collector returns :done immediately when store is empty" do
      {:ok, collector} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [], :done} = collector.(100)
    end

    test "collector returns all metrics with :infinity limit" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 5, %{id: "a"})

      {:ok, collector} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [%{name: "pull.test.sum"}], :done} = collector.(:infinity)
    end

    test "bounded drain: collector returns {:more, next} when rows remain" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})
      MetricStore.write_metric(@name, metric, 3, %{id: "c"})

      {:ok, collector} = MetricStore.prepare_to_collect(@name)

      # limit 1 — one row comes back, two remain
      assert {:ok, batch1, {:more, c2}} = collector.(1)
      assert length(batch1) == 1

      # limit 10 — drains the remaining two rows in one shot
      assert {:ok, batch2, :done} = c2.(10)
      assert length(batch2) == 1

      # total data points across both batches
      total_points =
        (batch1 ++ batch2)
        |> Enum.flat_map(fn m -> elem(m.data, 1).data_points end)
        |> length()

      assert total_points == 3
    end

    test "after :done, next prepare_to_collect starts a fresh generation" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 10, %{id: "x"})

      {:ok, c1} = MetricStore.prepare_to_collect(@name)
      assert {:ok, _, :done} = c1.(:infinity)

      # Nothing new written — next collector finds an empty generation
      {:ok, c2} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [], :done} = c2.(:infinity)
    end

    test "writes after prepare_to_collect are isolated from the current collector" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})

      # Acquire seals gen 0; writes now go to gen 1
      {:ok, collector} = MetricStore.prepare_to_collect(@name)

      MetricStore.write_metric(@name, metric, 99, %{id: "new"})

      # Drain gen 0 row by row — "new" must not appear
      assert {:ok, batch1, {:more, c2}} = collector.(1)
      assert length(batch1) == 1

      assert {:ok, batch2, :done} = c2.(1)
      assert length(batch2) == 1

      # Next cycle picks up the "new" write from gen 1
      {:ok, c3} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [metric3], :done} = c3.(:infinity)

      [dp] = elem(metric3.data, 1).data_points
      assert dp.value == {:as_int, 99}
    end

    test "full lifecycle across two generations" do
      metric = Metrics.sum("pull.test.sum")
      MetricStore.write_metric(@name, metric, 10, %{id: "g0"})

      {:ok, c0} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [m0], :done} = c0.(:infinity)
      assert [%{value: {:as_int, 10}}] = elem(m0.data, 1).data_points

      MetricStore.write_metric(@name, metric, 20, %{id: "g1"})

      {:ok, c1} = MetricStore.prepare_to_collect(@name)
      assert {:ok, [m1], :done} = c1.(:infinity)
      assert [%{value: {:as_int, 20}}] = elem(m1.data, 1).data_points
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
