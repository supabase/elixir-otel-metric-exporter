defmodule OtelMetricExporter.MetricStoreTest do
  use ExUnit.Case, async: true
  import ExUnit.CaptureLog

  alias OtelMetricExporter.MetricStore

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  setup do
    bypass = Bypass.open()
    {:ok, _} = start_supervised({Finch, name: TestFinch})

    config = %{
      otlp_protocol: :http_protobuf,
      otlp_endpoint: "http://localhost:#{bypass.port}",
      otlp_headers: %{},
      otlp_compression: nil,
      export_period: 1000,
      default_buckets: @default_buckets,
      metrics: [],
      finch_pool: TestFinch
    }

    start_supervised!({MetricStore, config})

    {:ok, bypass: bypass}
  end

  describe "record_metric/3" do
    test "records counter metrics" do
      metric_name = "test.counter"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :counter, values: %{^tags => 3}}} =
               MetricStore.get_metrics()
    end

    test "records sum metrics" do
      metric_name = "test.sum"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :sum, values: %{^tags => 3}}} = MetricStore.get_metrics()
    end

    test "records last value metrics" do
      metric_name = "test.last_value"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :last_value, values: %{^tags => 2}}} =
               MetricStore.get_metrics()
    end

    test "records distribution metrics" do
      metric_name = "test.distribution"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :distribution, values: %{^tags => [1, 2]}}} =
               MetricStore.get_metrics()
    end

    test "handles different tag sets independently" do
      metric_name = "test.counter"
      tags1 = %{test: "value1"}
      tags2 = %{test: "value2"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags1})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags2})

      assert %{
               ^metric_name => %{
                 type: :counter,
                 values: %{^tags1 => 1, ^tags2 => 2}
               }
             } = MetricStore.get_metrics()
    end
  end

  describe "export flow" do
    test "exports metrics in protobuf format", %{bypass: bypass} do
      metric_name = "test.counter"
      tags = %{test: "value"}

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        {:ok, body, conn} = Plug.Conn.read_body(conn)

        assert {"content-type", "application/x-protobuf"} in conn.req_headers
        assert {"accept", "application/x-protobuf"} in conn.req_headers

        assert body != ""

        Plug.Conn.resp(conn, 200, "")
      end)

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      # Export metrics
      metrics_before = MetricStore.get_metrics()
      assert map_size(metrics_before) > 0

      send(MetricStore, :export)
      # Give it time to process the export
      Process.sleep(100)

      # Verify metrics were cleared
      assert MetricStore.get_metrics() == %{}
    end

    test "handles server errors gracefully", %{bypass: bypass} do
      metric_name = "test.counter"
      tags = %{test: "value"}

      Bypass.expect_once(bypass, "POST", "/v1/metrics", fn conn ->
        Plug.Conn.resp(conn, 500, "Internal Server Error")
      end)

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})

      # Export metrics
      metrics_before = MetricStore.get_metrics()
      assert map_size(metrics_before) > 0

      log =
        capture_log([level: :error], fn ->
          send(MetricStore, :export)
          # Give it time to process the export
          Process.sleep(200)
        end)

      assert log =~ "Failed to export metrics: {:unexpected_status, %Finch.Response{status: 500"

      # Verify metrics were not cleared due to error
      assert MetricStore.get_metrics() == metrics_before
    end

    test "handles connection errors gracefully", %{bypass: bypass} do
      metric_name = "test.counter"
      tags = %{test: "value"}

      Bypass.down(bypass)

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})

      # Export metrics
      metrics_before = MetricStore.get_metrics()
      assert map_size(metrics_before) > 0

      log =
        capture_log([level: :error], fn ->
          send(MetricStore, :export)
          # Give it time to process the export
          Process.sleep(100)
        end)

      assert log =~ "Failed to export metrics: %Mint.TransportError{reason: :econnrefused}"

      # Verify metrics were not cleared due to error
      assert MetricStore.get_metrics() == metrics_before
    end

    test "uses configured buckets for distributions" do
      metric_name = "test.distribution"
      tags = %{test: "value"}
      custom_buckets = [0, 10, 100]

      GenServer.cast(MetricStore, {:record_metric, metric_name, 5, tags, custom_buckets})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 50, tags, custom_buckets})

      assert %{
               ^metric_name => %{
                 type: :distribution,
                 values: %{^tags => [5, 50]},
                 buckets: ^custom_buckets
               }
             } = MetricStore.get_metrics()
    end

    test "falls back to default buckets when none provided" do
      metric_name = "test.distribution"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 5, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 50, tags})

      assert %{
               ^metric_name => %{
                 type: :distribution,
                 values: %{^tags => [5, 50]},
                 buckets: @default_buckets
               }
             } = MetricStore.get_metrics()
    end
  end
end
