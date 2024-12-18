defmodule OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :resource_metrics, 1,
    repeated: true,
    type: OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.ResourceMetrics,
    json_name: "resourceMetrics"
end

defmodule OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceResponse do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :partial_success, 1,
    type: OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsPartialSuccess,
    json_name: "partialSuccess"
end

defmodule OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsPartialSuccess do
  @moduledoc false

  use Protobuf, syntax: :proto3, protoc_gen_elixir_version: "0.13.0"

  field :rejected_data_points, 1, type: :int64, json_name: "rejectedDataPoints"
  field :error_message, 2, type: :string, json_name: "errorMessage"
end