defmodule OtelMetricExporter.HandlerConfig do
  @moduledoc false

  alias Telemetry.Metrics

  defmodule Metric do
    @moduledoc false

    defstruct [:id, :metric, :type, :name, :measurement, :keep, :tag_idx]
  end

  def compile(metrics, config) do
    extract_tags = config[:extract_tags]
    {tag_fns, tag_indices} = compile_tag_fns(metrics, extract_tags)

    %{
      metrics:
        metrics
        |> Enum.with_index()
        |> Enum.map(fn {metric, id} -> compile_metric(metric, id, tag_indices, extract_tags) end),
      tag_fns: List.to_tuple(tag_fns),
      name: config.name
    }
  end

  defp compile_tag_fns(metrics, extract_tags) do
    {tag_fns, tag_indices, _next_idx} =
      Enum.reduce(metrics, {[], %{}, 0}, fn metric, {tag_fns, tag_indices, next_idx} ->
        key = tag_fn_key(metric, extract_tags)

        case tag_indices do
          %{^key => _idx} ->
            {tag_fns, tag_indices, next_idx}

          %{} ->
            tag_fn = compile_tag_fn(metric, extract_tags)
            {[tag_fn | tag_fns], Map.put(tag_indices, key, next_idx), next_idx + 1}
        end
      end)

    {Enum.reverse(tag_fns), tag_indices}
  end

  defp compile_metric(metric, id, tag_indices, extract_tags) do
    %Metric{
      id: id,
      metric: metric,
      type: metric_type(metric),
      name: Enum.join(metric.name, "."),
      measurement: metric.measurement,
      keep: metric.keep,
      tag_idx: Map.fetch!(tag_indices, tag_fn_key(metric, extract_tags))
    }
  end

  defp tag_fn_key(metric, nil), do: {metric.tag_values, metric.tags}
  defp tag_fn_key(metric, _extract_tags), do: {:custom, metric}

  defp compile_tag_fn(%{tags: []}, nil), do: fn _metadata -> %{} end

  defp compile_tag_fn(metric, nil) do
    fn metadata ->
      metadata
      |> metric.tag_values.()
      |> Map.take(metric.tags)
    end
  end

  defp compile_tag_fn(metric, extract_tags) do
    fn metadata -> extract_tags.(metric, metadata) end
  end

  defp metric_type(%Metrics.Counter{}), do: :counter
  defp metric_type(%Metrics.Sum{}), do: :sum
  defp metric_type(%Metrics.LastValue{}), do: :last_value
  defp metric_type(%Metrics.Distribution{}), do: :distribution
end
