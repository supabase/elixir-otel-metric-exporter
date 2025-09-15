defmodule OtelMetricExporter.OtelApiTest do
  use ExUnit.Case, async: false
  alias OtelMetricExporter.OtelApi
  alias OtelMetricExporter.OtelApi.Config
  alias OtelMetricExporter.Opentelemetry.Proto.Logs.V1.LogRecord
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.Metric

  setup do
    on_exit(fn ->
      System.delete_env("OTEL_SERVICE_NAME")
      System.delete_env("OTEL_RESOURCE_ATTRIBUTES")
    end)
  end

  describe "new/1" do
    test "creates a new OtelApi struct" do
      assert {:ok, %OtelApi{config: %Config{otlp_endpoint: "http://localhost:4317"}}, %{}} =
               OtelApi.new(%{finch: :test_finch, otlp_endpoint: "http://localhost:4317"}, :logs)
    end

    test "returns unrecognized options" do
      assert {:ok, %OtelApi{}, %{unknown_option: "value"}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   unknown_option: "value"
                 },
                 :logs
               )
    end

    test "normalizes the resource" do
      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{service: %{name: "test"}}
                 },
                 :logs
               )
    end

    test "puts service name from env" do
      System.put_env("OTEL_SERVICE_NAME", "test")

      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{}
                 },
                 :logs
               )
    end

    test "gives priority to provided config over env for service name" do
      System.put_env("OTEL_SERVICE_NAME", "test")

      assert {:ok, %OtelApi{config: %Config{resource: %{"service.name" => "test2"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{service: %{name: "test2"}}
                 },
                 :logs
               )
    end

    test "puts resource attributes from env" do
      System.put_env("OTEL_RESOURCE_ATTRIBUTES", "test=test2,test2=test3")

      assert {:ok, %OtelApi{config: %Config{resource: %{"test" => "test2", "test2" => "test3"}}},
              %{}} =
               OtelApi.new(%{finch: :test_finch, otlp_endpoint: "http://localhost:4317"}, :logs)
    end

    test "gives priority to provided config over env for resource attributes" do
      System.put_env("OTEL_RESOURCE_ATTRIBUTES", "test=test2")

      assert {:ok, %OtelApi{config: %Config{resource: %{"test" => "test3"}}}, %{}} =
               OtelApi.new(
                 %{
                   finch: :test_finch,
                   otlp_endpoint: "http://localhost:4317",
                   resource: %{test: "test3"}
                 },
                 :metrics
               )
    end
  end

  describe "send_metrics/2" do
    test "if :config has export_callback, executes it instead of sending HTTP request" do
      pid = self()

      callback =
        fn {:metrics, [metric]}, %Config{} ->
          send(pid, metric.description)
        end

      metrics = [%Metric{description: "callback executed"}]

      %OtelApi{config: %Config{export_callback: callback}}
      |> OtelApi.send_metrics(metrics)

      assert_received "callback executed"
    end
  end

  describe "send_log_events/2" do
    test "if :config has export_callback, executes it instead of sending HTTP request" do
      pid = self()

      callback =
        fn {:logs, [log]}, %Config{} ->
          {:string_value, message} = log.body.value

          send(pid, message)
        end

      logs = [%LogRecord{body: %{value: {:string_value, "callback executed"}}}]

      %OtelApi{config: %Config{export_callback: callback}}
      |> OtelApi.send_log_events(logs)

      assert_received "callback executed"
    end
  end
end
