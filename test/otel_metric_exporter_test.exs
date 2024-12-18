defmodule OtelMetricExporterTest do
  use ExUnit.Case
  doctest OtelMetricExporter

  test "greets the world" do
    assert OtelMetricExporter.hello() == :world
  end
end
