defmodule OtelMetricExporter.MetricStoreTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.MetricStore
  alias Telemetry.Metrics

  @default_buckets [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]

  setup do
    config = %{
      otlp_protocol: :http_protobuf,
      otlp_endpoint: "http://localhost:4318",
      otlp_headers: %{},
      otlp_compression: nil,
      export_period: 1000,
      default_buckets: @default_buckets
    }

    start_supervised!({MetricStore, config})

    :ok
  end

  describe "record_metric/3" do
    test "records counter metrics" do
      metric_name = "test.counter"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :counter, values: %{^tags => 3}}} = MetricStore.get_metrics()
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

      assert %{^metric_name => %{type: :last_value, values: %{^tags => 2}}} = MetricStore.get_metrics()
    end

    test "records distribution metrics" do
      metric_name = "test.distribution"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      assert %{^metric_name => %{type: :distribution, values: %{^tags => [1, 2]}}} = MetricStore.get_metrics()
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
    test "exports metrics and clears them" do
      metric_name = "test.counter"
      tags = %{test: "value"}

      GenServer.cast(MetricStore, {:record_metric, metric_name, 1, tags})
      GenServer.cast(MetricStore, {:record_metric, metric_name, 2, tags})

      # Export metrics
      metrics_before = MetricStore.get_metrics()
      assert map_size(metrics_before) > 0

      send(MetricStore, :export)
      Process.sleep(100)  # Give it time to process the export

      # Verify metrics were cleared
      metrics_after = MetricStore.get_metrics()
      assert map_size(metrics_after) == 0
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
