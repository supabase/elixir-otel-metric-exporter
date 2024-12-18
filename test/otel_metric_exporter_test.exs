defmodule OtelMetricExporterTest do
  use ExUnit.Case, async: false  # Not async due to telemetry event handling

  alias OtelMetricExporter.MetricStore

  @default_config [
    otlp_protocol: :http_protobuf,
    otlp_endpoint: "http://localhost:4318",
    otlp_headers: %{},
    otlp_compression: nil,
    export_period: 1000,
    metrics: []
  ]

  describe "start_link/1" do
    test "starts with valid config" do
      metrics = [
        Telemetry.Metrics.counter("test.counter", tags: [:test]),
        Telemetry.Metrics.sum("test.sum", tags: [:test]),
        Telemetry.Metrics.last_value("test.last_value", tags: [:test]),
        Telemetry.Metrics.distribution("test.distribution", tags: [:test])
      ]

      config = Keyword.put(@default_config, :metrics, metrics)
      assert {:ok, pid} = OtelMetricExporter.start_link(config)
      assert Process.alive?(pid)
    end

    test "fails with invalid config" do
      assert {:error, _} = OtelMetricExporter.start_link([])
      assert {:error, _} = OtelMetricExporter.start_link(otlp_protocol: :invalid)
    end
  end

  describe "telemetry integration" do
    setup do
      metrics = [
        Telemetry.Metrics.counter("test.metric", event_name: [:test, :event], measurement: :value, tags: [:test]),
        Telemetry.Metrics.sum("test.metric", event_name: [:test, :event], measurement: :value, tags: [:test]),
        Telemetry.Metrics.last_value("test.metric", event_name: [:test, :event], measurement: :value, tags: [:test]),
        Telemetry.Metrics.distribution("test.metric", event_name: [:test, :event], measurement: :value, tags: [:test])
      ]

      config = Keyword.put(@default_config, :metrics, metrics)
      start_supervised!({OtelMetricExporter, config})

      :ok
    end

    test "handles telemetry events" do
      :telemetry.execute([:test, :event], %{value: 42}, %{test: "value"})
      Process.sleep(100)  # Give it time to process

      metrics = MetricStore.get_metrics()

      # Check that all metric types were recorded
      for name <- Map.keys(metrics) do
        metric = metrics[name]
        assert %{test: "value"} in Map.keys(metric.values)
      end
    end

    test "handles events with keep function" do
      metrics = [
        Telemetry.Metrics.counter(
          "test.filtered",
          event_name: [:test, :filtered],
          measurement: :value,
          tags: [:test],
          keep: &(&1.test == "keep")
        )
      ]

      config = Keyword.put(@default_config, :metrics, metrics)
      start_supervised!({OtelMetricExporter, config})

      # This one should be kept
      :telemetry.execute([:test, :filtered], %{value: 1}, %{test: "keep"})
      # This one should be filtered out
      :telemetry.execute([:test, :filtered], %{value: 2}, %{test: "drop"})

      Process.sleep(100)  # Give it time to process

      metrics = MetricStore.get_metrics()
      assert get_in(metrics, ["test.filtered.value", :values, %{test: "keep"}]) == 1
      assert get_in(metrics, ["test.filtered.value", :values, %{test: "drop"}]) == nil
    end

    test "handles measurement functions" do
      metrics = [
        Telemetry.Metrics.counter(
          "test.measured",
          event_name: [:test, :measured],
          measurement: fn measurements -> measurements.value * 2 end,
          tags: [:test]
        ),
        Telemetry.Metrics.counter(
          "test.measured_with_metadata",
          event_name: [:test, :measured],
          measurement: fn measurements, metadata -> measurements.value * metadata.multiplier end,
          tags: [:test]
        )
      ]

      config = Keyword.put(@default_config, :metrics, metrics)
      start_supervised!({OtelMetricExporter, config})

      :telemetry.execute([:test, :measured], %{value: 21}, %{test: "value", multiplier: 3})

      Process.sleep(100)  # Give it time to process

      metrics = MetricStore.get_metrics()
      assert get_in(metrics, ["test.measured.value", :values, %{test: "value"}]) == 42
      assert get_in(metrics, ["test.measured_with_metadata.value", :values, %{test: "value"}]) == 63
    end

    test "handles tag functions" do
      metrics = [
        Telemetry.Metrics.counter(
          "test.tags",
          event_name: [:test, :tags],
          measurement: :value,
          tags: [:dynamic],
          tag_values: fn metadata -> Map.put(metadata, :dynamic, "computed_#{metadata.input}") end
        )
      ]

      config = Keyword.put(@default_config, :metrics, metrics)
      start_supervised!({OtelMetricExporter, config})

      :telemetry.execute([:test, :tags], %{value: 42}, %{input: "test"})

      Process.sleep(100)  # Give it time to process

      metrics = MetricStore.get_metrics()
      assert get_in(metrics, ["test.tags.value", :values, %{dynamic: "computed_test"}]) == 42
    end
  end
end
