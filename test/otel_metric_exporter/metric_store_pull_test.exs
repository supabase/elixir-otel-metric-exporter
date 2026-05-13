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

  describe "pull/1" do
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

      # pull/1 rotates the generation before collecting; writes after rotation
      # go into the new generation and should appear in a subsequent pull
      assert {:ok, first} = MetricStore.pull(@name)
      assert length(first) == 1

      # Write into the new generation
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

  describe "timer disarming in pull mode" do
    test "no :export message fired after export_period when pull_mode is true" do
      config = %{
        export_period: 50,
        metrics: [],
        name: @name,
        pull_mode: true
      }

      pid = start_supervised!({MetricStore, config})

      # Wait longer than export_period
      Process.sleep(200)

      # The GenServer mailbox should not have accumulated :export messages
      {:messages, msgs} = Process.info(pid, :messages)
      refute :export in msgs
    end
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
