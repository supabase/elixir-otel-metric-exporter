# Run with: mix run bench/find_bucket_bench.exs

# Micro-benchmark for the find_bucket/2 algorithm in
# OtelMetricExporter.MetricStore. The function is private; we inline the
# same logic here so the bench doesn't depend on production internals.

defmodule BenchFindBucket do
  def find_bucket(bucket_bounds, value) do
    case Enum.find_index(bucket_bounds, &(value <= &1)) do
      # Overflow bucket
      nil -> length(bucket_bounds)
      idx -> idx
    end
  end
end

default_buckets = [0, 5, 10, 25, 50, 75, 100, 250, 500, 750, 1000, 2500, 5000, 7500, 10000]
small_buckets = [0, 10, 100, 1000, 10000]
large_buckets = Enum.map(0..31, fn i -> i * 500 end)

# For each input, prepare a representative spread of values that hits every
# bucket (including overflow).
build_values = fn buckets ->
  max = List.last(buckets)
  step = max(1, div(max, length(buckets)))
  values = for i <- 0..length(buckets), do: i * step
  # Also include explicit boundary values.
  values ++ buckets ++ [max + 1]
end

inputs = %{
  "small (5 buckets)" => {small_buckets, build_values.(small_buckets)},
  "default (15 buckets)" => {default_buckets, build_values.(default_buckets)},
  "large (32 buckets)" => {large_buckets, build_values.(large_buckets)}
}

job = fn {buckets, values} ->
  Enum.each(values, fn v -> BenchFindBucket.find_bucket(buckets, v) end)
end

Benchee.run(
  %{"find_bucket" => job},
  inputs: inputs,
  time: 3,
  warmup: 1,
  print: [fast_warning: false]
)
