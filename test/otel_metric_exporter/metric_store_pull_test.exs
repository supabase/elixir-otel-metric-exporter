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

  describe "pull/2 with limit" do
    setup %{store_config: config} do
      metric = Metrics.sum("pull.test.sum")
      config = %{config | metrics: [metric]}
      {:ok, store: start_supervised!({MetricStore, config}), metric: metric}
    end

    test "limits the number of ETS rows returned per call" do
      metric = Metrics.sum("pull.test.sum")

      # Write 3 separate tag sets — each is a distinct ETS row
      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})
      MetricStore.write_metric(@name, metric, 3, %{id: "c"})

      # Limit to 1 row — only one tag set should come back
      assert {:ok, first} = MetricStore.pull(@name, 1)
      assert length(first) == 1

      # Two rows still remain in the partially-drained generation
      assert {:ok, second} = MetricStore.pull(@name, 2)
      assert length(second) == 1

      # Generation is now fully drained
      assert {:ok, []} = MetricStore.pull(@name, 10)
    end

    test "resumes the same generation on successive bounded pulls" do
      metric = Metrics.sum("pull.test.sum")

      MetricStore.write_metric(@name, metric, 10, %{id: "x"})
      MetricStore.write_metric(@name, metric, 20, %{id: "y"})

      assert {:ok, _batch1} = MetricStore.pull(@name, 1)
      assert {:ok, _batch2} = MetricStore.pull(@name, 1)

      # After two single-row pulls the generation should be empty
      assert {:ok, []} = MetricStore.pull(@name, 1)
    end

    test "pull with limit larger than available rows drains completely" do
      metric = Metrics.sum("pull.test.sum")

      MetricStore.write_metric(@name, metric, 5, %{id: "only"})

      assert {:ok, metrics} = MetricStore.pull(@name, 1000)
      assert length(metrics) == 1
      assert {:ok, []} = MetricStore.pull(@name, 1000)
    end

    test "pull(:infinity) still works and drains whole generation" do
      metric = Metrics.sum("pull.test.sum")

      MetricStore.write_metric(@name, metric, 1, %{id: "a"})
      MetricStore.write_metric(@name, metric, 2, %{id: "b"})

      assert {:ok, metrics} = MetricStore.pull(@name, :infinity)
      assert length(metrics) == 1

      assert {:ok, []} = MetricStore.pull(@name, :infinity)
    end
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
