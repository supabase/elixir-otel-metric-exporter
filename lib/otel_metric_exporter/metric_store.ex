defmodule OtelMetricExporter.MetricStore do
  @moduledoc false
  use GenServer
  require Logger

  alias OtelMetricExporter.Opentelemetry.Proto.Collector.Metrics.V1.ExportMetricsServiceRequest
  alias OtelMetricExporter.Opentelemetry.Proto.Metrics.V1.{
    ResourceMetrics,
    ScopeMetrics,
    Metric,
    NumberDataPoint,
    HistogramDataPoint,
    Gauge,
    Sum,
    Histogram,
    AggregationTemporality
  }
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.KeyValue
  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.InstrumentationScope

  defmodule State do
    @moduledoc false
    defstruct [:config, metrics: %{}]

    @type t :: %__MODULE__{
            config: map(),
            metrics: %{
              optional(String.t()) => %{
                metric: Telemetry.Metrics.t(),
                type: :counter | :sum | :last_value | :distribution,
                values: %{
                  optional(map()) => number() | [number()]
                },
                buckets: [number()] | nil
              }
            }
          }
  end

  def start_link(config) do
    GenServer.start_link(__MODULE__, config, name: __MODULE__)
  end

  @impl true
  def init(config) do
    # Set up periodic export
    if config.export_period > 0 do
      Process.send_after(self(), :export_metrics, config.export_period)
    end

    {:ok, %State{config: config, metrics: init_metrics(config.metrics)}}
  end

  @impl true
  def handle_info(:export_metrics, %{config: config} = state) do
    # Schedule next export
    if config.export_period > 0 do
      Process.send_after(self(), :export_metrics, config.export_period)
    end

    now = System.system_time(:nanosecond)

    state.metrics
    |> build_metrics_for_export(now)
    |> build_export_request()
    |> export_request(config)
    |> handle_export_result(state)
  end

  @impl true
  def handle_cast({:record_metric, name, value, tags}, state) do
    case Map.get(state.metrics, name) do
      nil ->
        Logger.warning("Received value for unknown metric #{name}")
        {:noreply, state}

      metric_def ->
        {:noreply, %{state | metrics: update_metric_value(metric_def, name, value, tags, state.metrics)}}
    end
  end

  defp init_metrics(metrics) do
    metrics
    |> Enum.map(&init_metric/1)
    |> Enum.reject(&is_nil/1)
    |> Map.new()
  end

  defp init_metric(metric) do
    type =
      case metric do
        %Telemetry.Metrics.Counter{} -> :counter
        %Telemetry.Metrics.Sum{} -> :sum
        %Telemetry.Metrics.LastValue{} -> :last_value
        %Telemetry.Metrics.Distribution{} -> :distribution
        other ->
          Logger.warning(
            "Unsupported metric type #{inspect(other)}. Only counter, sum, last_value and distribution are supported"
          )

          nil
      end

    if type do
      buckets =
        if type == :distribution do
          get_in(metric.reporter_options, [:buckets])
        end

      {metric.name,
       %{
         metric: metric,
         type: type,
         values: %{},
         buckets: buckets
       }}
    end
  end

  defp update_metric_value(metric_def, name, value, tags, metrics) do
    updated =
      case metric_def.type do
        :counter ->
          update_in(metric_def.values[tags], fn
            nil -> value
            existing -> existing + value
          end)

        :sum ->
          update_in(metric_def.values[tags], fn
            nil -> value
            existing -> existing + value
          end)

        :last_value ->
          put_in(metric_def.values[tags], value)

        :distribution ->
          update_in(metric_def.values[tags], fn
            nil -> [value]
            existing -> [value | existing]
          end)
      end

    Map.put(metrics, name, updated)
  end

  defp build_metrics_for_export(metrics, now) do
    metrics
    |> Enum.map(fn {name, metric_def} ->
      %Metric{
        name: name,
        description: metric_def.metric.description || "",
        unit: metric_def.metric.unit || "",
        data: build_metric_data(metric_def, now)
      }
    end)
  end

  defp build_metric_data(%{type: :distribution, buckets: buckets, values: values} = metric_def, now) do
    bounds = buckets || metric_def.config.default_buckets

    {:histogram,
      %Histogram{
        data_points: build_histogram_points(values, bounds, now),
        aggregation_temporality: AggregationTemporality.value(:AGGREGATION_TEMPORALITY_DELTA)
      }}
  end

  defp build_metric_data(%{type: :counter, values: values}, now) do
    {:sum,
      %Sum{
        data_points: build_number_points(values, now),
        aggregation_temporality: AggregationTemporality.value(:AGGREGATION_TEMPORALITY_DELTA),
        is_monotonic: true
      }}
  end

  defp build_metric_data(%{type: :sum, values: values}, now) do
    {:sum,
      %Sum{
        data_points: build_number_points(values, now),
        aggregation_temporality: AggregationTemporality.value(:AGGREGATION_TEMPORALITY_DELTA),
        is_monotonic: false
      }}
  end

  defp build_metric_data(%{type: :last_value, values: values}, now) do
    {:gauge, %Gauge{data_points: build_number_points(values, now)}}
  end

  defp build_export_request(metrics) do
    %ExportMetricsServiceRequest{
      resource_metrics: [
        %ResourceMetrics{
          scope_metrics: [
            %ScopeMetrics{
              scope: %InstrumentationScope{
                name: "otel_metric_exporter",
                version: "1.0.0"
              },
              metrics: metrics
            }
          ]
        }
      ]
    }
  end

  defp handle_export_result(:ok, state) do
    {:noreply, %{state | metrics: clear_delta_metrics(state.metrics)}}
  end

  defp handle_export_result({:error, reason}, state) do
    Logger.error("Failed to export metrics: #{inspect(reason)}")
    {:noreply, state}
  end

  # Reset delta metrics after export
  defp clear_delta_metrics(metrics) do
    metrics
    |> Enum.map(fn {name, metric_def} ->
      case metric_def.type do
        type when type in [:counter, :sum, :distribution] ->
          {name, %{metric_def | values: %{}}}

        _other ->
          {name, metric_def}
      end
    end)
    |> Map.new()
  end

  defp build_number_points(values, now) do
    values
    |> Enum.map(fn {tags, value} ->
      %NumberDataPoint{
        attributes: build_attributes(tags),
        time_unix_nano: now,
        value: {:as_double, value}
      }
    end)
  end

  defp build_histogram_points(values, bounds, now) do
    values
    |> Enum.map(fn {tags, measurements} ->
      {buckets, sum} = calculate_histogram_buckets(measurements, bounds)

      %HistogramDataPoint{
        attributes: build_attributes(tags),
        time_unix_nano: now,
        count: length(measurements),
        sum: sum,
        bucket_counts: buckets,
        explicit_bounds: bounds
      }
    end)
  end

  defp calculate_histogram_buckets(measurements, bounds) do
    measurements
    |> Enum.reduce({List.duplicate(0, length(bounds) + 1), 0}, fn value, {buckets, sum} ->
      bucket_index =
        bounds
        |> Enum.find_index(fn bound -> value <= bound end)
        |> Kernel.||(length(bounds))

      updated_buckets =
        List.update_at(buckets, bucket_index, &(&1 + 1))

      {updated_buckets, sum + value}
    end)
  end

  defp build_attributes(tags) do
    tags
    |> Enum.map(fn {key, value} ->
      %KeyValue{
        key: to_string(key),
        value: {:string_value, to_string(value)}
      }
    end)
  end

  defp export_request(request, config) do
    encoded = ExportMetricsServiceRequest.encode(request)
    headers = build_headers(config)
    body = maybe_compress(encoded, config.otlp_compression)

    Finch.build(
      :post,
      "#{config.otlp_endpoint}/v1/metrics",
      Map.to_list(headers),
      body
    )
    |> Finch.request(Module.concat(__MODULE__, Finch))
    |> handle_response()
  end

  defp build_headers(config) do
    headers = Map.put(config.otlp_headers, "content-type", "application/x-protobuf")

    if config.otlp_compression == :gzip do
      Map.put(headers, "content-encoding", "gzip")
    else
      headers
    end
  end

  defp maybe_compress(data, :gzip), do: :zlib.gzip(data)
  defp maybe_compress(data, _), do: data

  defp handle_response({:ok, %{status: status}}) when status in 200..299, do: :ok
  defp handle_response({:ok, response}), do: {:error, {:unexpected_response, response}}
  defp handle_response({:error, reason}), do: {:error, reason}
end
