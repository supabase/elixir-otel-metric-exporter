defmodule OtelMetricExporter.MetricStoreTest do
  use ExUnit.Case, async: false
  import ExUnit.CaptureLog

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias Telemetry.Metrics
  alias OtelMetricExporter.MetricStore

  @name :metric_store_test
  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  setup do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    config = %{
      otlp_protocol: :http_protobuf,
      otlp_endpoint: "http://localhost:#{bypass.port}",
      otlp_headers: %{},
      otlp_compression: nil,
      resource: %{instance: %{id: "test"}},
      export_period: 1000,
      default_buckets: @default_buckets,
      metrics: [],
      finch_pool: TestFinch,
      retry: false,
      name: @name
    }

    {:ok, bypass: bypass, store_config: config}
  end

  describe "recording metrics" do
    setup %{store_config: config}, do: {:ok, store: start_supervised!({MetricStore, config})}

    test "records counter metrics" do
      metric = Metrics.counter("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:counter, "test.value"} => %{^tags => 2}} = metrics
    end

    test "records sum metrics" do
      metric = Metrics.sum("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:sum, "test.value"} => %{^tags => 3}} = metrics
    end

    test "records last value metrics" do
      metric = Metrics.last_value("test.value")
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{{:last_value, "test.value"} => %{^tags => 2}} = metrics
    end

    test "records distribution metrics" do
      metric = Metrics.distribution("test.value", reporter_options: [buckets: [2, 4]])
      tags = %{test: "value"}

      MetricStore.write_metric(@name, metric, 2, tags)
      MetricStore.write_metric(@name, metric, 3, tags)
      MetricStore.write_metric(@name, metric, 5, tags)
      MetricStore.write_metric(@name, metric, 5, tags)

      metrics = MetricStore.get_metrics(@name)

      assert %{
               {:distribution, "test.value"} => %{
                 ^tags => %{0 => {1, 2}, 1 => {1, 3}, 2 => {2, 10}}
               }
             } = metrics
    end

    test "handles different tag sets independently" do
      metric = Metrics.sum("test.value")
      tags1 = %{test: "value1"}
      tags2 = %{test: "value2"}

      MetricStore.write_metric(@name, metric, 1, tags1)
      MetricStore.write_metric(@name, metric, 2, tags2)
      MetricStore.write_metric(@name, metric, 2, tags1)

      metrics = MetricStore.get_metrics(@name)

      assert %{
               {:sum, "test.value"} => %{^tags1 => 3, ^tags2 => 2}
             } = metrics
    end
  end

  describe "storage variants" do
    test "records metrics in striped ETS storage", %{store_config: base_config} do
      metric = Metrics.sum("test.striped.value")
      name = :metric_store_striped_test

      config =
        Map.merge(base_config, %{
          name: name,
          metrics: [metric],
          storage: :striped
        })

      start_supervised!({MetricStore, config})

      MetricStore.write_metric(name, metric, 1, %{})
      MetricStore.write_metric(name, metric, 2, %{})

      assert %{{:sum, "test.striped.value"} => %{%{} => 3}} = MetricStore.get_metrics(name)
    end

    test "exports atomics-backed distribution metrics", %{store_config: base_config} do
      metric =
        Metrics.distribution("test.atomics.distribution", reporter_options: [buckets: [2, 4]])

      name = :metric_store_atomics_test
      test_pid = self()

      callback = fn payload, _config ->
        send(test_pid, payload)
        :ok
      end

      config =
        Map.merge(base_config, %{
          name: name,
          metrics: [metric],
          distribution_storage: :atomics,
          export_callback: callback
        })

      start_supervised!({MetricStore, config})

      MetricStore.write_metric(name, metric, 2, %{})
      MetricStore.write_metric(name, metric, 5, %{})

      assert :ok = MetricStore.export_sync(name)

      assert_received {:metrics, [exported]}
      assert {:histogram, %{data_points: [point]}} = exported.data
      assert point.count == 2
      assert point.sum == 7
      assert point.bucket_counts == [1, 0, 1]
    end
  end

  describe "limit memory usage" do
    setup %{bypass: bypass, store_config: base_config} do
      Bypass.stub(
        bypass,
        "POST",
        "/v1/metrics",
        &Plug.Conn.resp(&1, 500, "")
      )

      metric = Metrics.sum("test.value")
      updated_config = %{base_config | metrics: [metric]}

      {:ok, store_config: updated_config, metric: metric}
    end

    defp induce_rotate_generation do
      capture_log(fn ->
        send(@name, :export)
        :timer.sleep(100)
      end)
    end

    test "gen rotation deletes oldest gen when threshold is surpassed", %{
      store_config: base_config,
      metric: metric
    } do
      config = Map.put(base_config, :max_table_memory, 3200)
      start_supervised!({MetricStore, config})

      MetricStore.write_metric(@name, metric, 1, %{})

      # first gen don't get deleted if the threshold were not violated yet

      induce_rotate_generation()

      refute MetricStore.get_metrics(@name, 0) == %{}

      # gets deleted at rotation when threshold was violated

      MetricStore.write_metric(@name, metric, 1, %{})

      induce_rotate_generation()

      assert MetricStore.get_metrics(@name, 0) == %{}
      refute MetricStore.get_metrics(@name, 1) == %{}
    end

    test "deletes older generations until threshold is respected", %{
      store_config: base_config,
      metric: metric
    } do
      config = Map.put(base_config, :max_table_memory, 3800)
      start_supervised!({MetricStore, config})

      # create multiple lightweight generations

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})
      induce_rotate_generation()

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})
      induce_rotate_generation()

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})
      induce_rotate_generation()

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})
      induce_rotate_generation()

      # make one generation with lots of metrics to force deleting multiple lightier ones

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})
      MetricStore.write_metric(@name, metric, 1, %{"test" => 2})
      MetricStore.write_metric(@name, metric, 1, %{"test" => 3})

      # drop until meeting threshold again

      induce_rotate_generation()

      assert MetricStore.get_metrics(@name, 0) == %{}
      assert MetricStore.get_metrics(@name, 1) == %{}
      assert MetricStore.get_metrics(@name, 2) == %{}

      refute MetricStore.get_metrics(@name, 3) == %{}
      refute MetricStore.get_metrics(@name, 4) == %{}
    end

    test "threshold violation don't delete current generation when there is no older ones", %{
      store_config: base_config,
      metric: metric
    } do
      config = Map.put(base_config, :max_table_memory, 1)
      start_supervised!({MetricStore, config})

      MetricStore.write_metric(@name, metric, 1, %{"test" => 1})

      refute MetricStore.get_metrics(@name, 0) == %{}
    end
  end

  describe "export flow" do
    test "exports all metrics in protobuf format", %{bypass: bypass, store_config: config} do
      metric1 = Metrics.sum("test.sum")
      metric2 = Metrics.counter("test.counter")
      metric3 = Metrics.last_value("test.last_value")
      metric4 = Metrics.distribution("test.distribution")
      start_supervised!({MetricStore, %{config | metrics: [metric1, metric2, metric3, metric4]}})

      tags = %{test: "value"}

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert {"content-type", "application/x-protobuf"} in conn.req_headers
        assert {"accept", "application/x-protobuf"} in conn.req_headers

        assert body != ""

        # Decodes withouth raising
        ExportMetricsServiceRequest.decode(body)

        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric1, 1, tags)
      MetricStore.write_metric(@name, metric2, 2, tags)
      MetricStore.write_metric(@name, metric3, 3, tags)
      MetricStore.write_metric(@name, metric4, 4, tags)
      MetricStore.write_metric(@name, metric4, 2000, tags)

      metrics = MetricStore.get_metrics(@name)
      assert map_size(metrics) > 0

      # Export metrics synchronously
      assert :ok = MetricStore.export_sync(@name)

      # Verify metrics were cleared
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "handles server errors gracefully", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 500, "Internal Server Error")
      end)

      MetricStore.write_metric(@name, metric, 1, tags)

      metrics = MetricStore.get_metrics(@name)

      # Export metrics synchronously
      assert capture_log(fn -> MetricStore.export_sync(@name) end) =~ "Failed to export metrics"

      # Verify metrics were not cleared due to error
      assert MetricStore.get_metrics(@name, 0) == metrics
    end

    test "handles connection errors gracefully", %{bypass: bypass, store_config: config} do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      Bypass.down(bypass)

      MetricStore.write_metric(@name, metric, 1, tags)

      metrics = MetricStore.get_metrics(@name)

      # Export metrics synchronously
      assert capture_log(fn -> MetricStore.export_sync(@name) end) =~ "Failed to export metrics"

      # Verify metrics were not cleared due to error
      assert MetricStore.get_metrics(@name, 0) == metrics
    end

    test "preserves metrics across generations on failed exports", %{
      bypass: bypass,
      store_config: config
    } do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric]}})

      # First generation
      MetricStore.write_metric(@name, metric, 1, tags)

      # First export fails
      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 500, "Internal Server Error")
      end)

      capture_log(fn -> MetricStore.export_sync(@name) end)

      # Second generation
      MetricStore.write_metric(@name, metric, 2, tags)

      # Second export succeeds and should include both generations
      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)
        metrics = ExportMetricsServiceRequest.decode(body)

        # Verify that we have one metric with sum = 3 (1 from first generation + 2 from second)
        assert [%{scope_metrics: [%{metrics: [metric]}]}] = metrics.resource_metrics

        assert {:sum, %{data_points: [point1, point2]}} = metric.data
        assert {:as_int, 1} = point1.value
        assert {:as_int, 2} = point2.value

        assert point1.time_unix_nano < point2.time_unix_nano
        assert point2.start_time_unix_nano > point1.time_unix_nano

        Plug.Conn.resp(conn, 200, "")
      end)

      assert :ok = MetricStore.export_sync(@name)

      # Both generations should be cleared after successful export
      assert MetricStore.get_metrics(@name, 0) == %{}
      assert MetricStore.get_metrics(@name, 1) == %{}
    end

    test "clears all metrics on successful HTTP export", %{bypass: bypass, store_config: config} do
      metric1 = Metrics.counter("test.counter.1")
      metric2 = Metrics.counter("test.counter.2")
      start_supervised!({MetricStore, %{config | metrics: [metric1, metric2]}})

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 200, "")
      end)

      MetricStore.write_metric(@name, metric1, 1, %{})
      MetricStore.write_metric(@name, metric2, 1, %{})

      assert :ok = MetricStore.export_sync(@name)

      # All metrics cleared after successful export
      assert MetricStore.get_metrics(@name, 0) == %{}
    end

    test "retains all metrics on HTTP server error", %{bypass: bypass, store_config: config} do
      metric1 = Metrics.counter("test.counter.1")
      metric2 = Metrics.counter("test.counter.2")
      tags = %{test: "value"}
      start_supervised!({MetricStore, %{config | metrics: [metric1, metric2]}})

      MetricStore.write_metric(@name, metric1, 1, tags)
      MetricStore.write_metric(@name, metric2, 1, tags)

      metrics_before = MetricStore.get_metrics(@name, 0)

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 500, "Internal Server Error")
      end)

      log = capture_log(fn -> assert {:error, _} = MetricStore.export_sync(@name) end)

      assert log =~ "Failed to export metrics"

      # All metrics retained on failure (all-or-nothing semantics)
      assert MetricStore.get_metrics(@name, 0) == metrics_before
    end
  end

  describe "callback export" do
    test "callback is invoked exactly once with the full metric list", %{
      store_config: config
    } do
      metrics = Enum.map(1..8, &Metrics.counter("test.counter.#{&1}"))
      test_pid = self()

      callback = fn payload, _config ->
        send(test_pid, payload)
        :ok
      end

      config =
        Map.merge(config, %{export_callback: callback, metrics: metrics})

      start_supervised!({MetricStore, config})

      Enum.each(metrics, &MetricStore.write_metric(@name, &1, 1, %{}))

      assert :ok = MetricStore.export_sync(@name)

      assert_received {:metrics, all_metrics}
      assert length(all_metrics) == 8
      refute_received {:metrics, _all_metrics}
    end

    test "successful callback clears generations from metrics_table", %{store_config: config} do
      metric = Metrics.sum("test.sum")
      tags = %{test: "value"}
      test_pid = self()

      callback = fn payload, _config ->
        send(test_pid, payload)
        :ok
      end

      start_supervised!(
        {MetricStore, Map.merge(config, %{export_callback: callback, metrics: [metric]})}
      )

      MetricStore.write_metric(@name, metric, 1, tags)
      MetricStore.write_metric(@name, metric, 2, tags)

      # Confirm there's data before
      assert %{{:sum, "test.sum"} => %{^tags => 3}} = MetricStore.get_metrics(@name, 0)

      assert :ok = MetricStore.export_sync(@name)

      # Both generation 0 (drained) is cleared
      assert MetricStore.get_metrics(@name, 0) == %{}
    end
  end

  test "returns error", %{store_config: config} do
    metric = Metrics.sum("test.sum")
    tags = %{test: "value"}

    callback = fn _payload, _config ->
      {:error, :failed}
    end

    config =
      Map.merge(config, %{export_callback: callback, metrics: [metric]})

    start_supervised!({MetricStore, config})

    MetricStore.write_metric(@name, metric, 1, tags)
    MetricStore.write_metric(@name, metric, 2, tags)

    log =
      capture_log(fn ->
        assert {:error, :failed} = MetricStore.export_sync(@name)
      end)

    assert log =~ ":failed"

    # Metrics are retained across all generations (gen 0 contained the writes)
    assert %{{:sum, "test.sum"} => %{^tags => 3}} = MetricStore.get_metrics(@name, 0)
  end
end
