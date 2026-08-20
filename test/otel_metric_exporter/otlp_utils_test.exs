defmodule OtelMetricExporter.OtlpUtilsTest do
  use ExUnit.Case, async: true

  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{
    AnyValue,
    ArrayValue,
    KeyValue,
    KeyValueList
  }

  alias OtelMetricExporter.OtlpUtils

  test "encodes booleans as booleans rather than atoms" do
    assert {:bool_value, true} = OtlpUtils.to_kv_value(true)
    assert {:bool_value, false} = OtlpUtils.to_kv_value(false)
  end

  test "encodes a mixed pseudo-keyword list as an array without raising" do
    assert {:array_value,
            %ArrayValue{
              values: [
                %AnyValue{
                  value:
                    {:array_value,
                     %ArrayValue{
                       values: [
                         %AnyValue{value: {:string_value, "ok"}},
                         %AnyValue{value: {:int_value, 1}}
                       ]
                     }}
                },
                %AnyValue{value: {:string_value, "bad"}}
              ]
            }} = OtlpUtils.to_kv_value([{:ok, 1}, :bad])
  end

  test "encodes a list of pairs as a key-value list" do
    tuple_key = inspect({:tuple, :key})

    assert {:kvlist_value,
            %KeyValueList{
              values: [
                %KeyValue{
                  key: ^tuple_key,
                  value: %AnyValue{value: {:bool_value, true}}
                }
              ]
            }} = OtlpUtils.to_kv_value([{{:tuple, :key}, true}])
  end

  test "build_kv tolerates invalid keys, entries, binaries, and structs" do
    attributes =
      OtlpUtils.build_kv([
        {[0xD800], <<255>>},
        :not_a_pair,
        {{:tuple, :key}, MapSet.new([:a])}
      ])

    assert [invalid_unicode, struct_value] = attributes
    assert invalid_unicode.key == "[55296]"
    assert invalid_unicode.value.value == {:string_value, "<<255>>"}

    assert struct_value.key == inspect({:tuple, :key})
    assert {:string_value, map_set_value} = struct_value.value.value
    assert map_set_value =~ "MapSet"
  end

  test "build_kv safely handles malformed containers and improper lists" do
    assert [] = OtlpUtils.build_kv({:not, :attributes})

    assert [%KeyValue{key: "ok", value: %AnyValue{value: {:int_value, 1}}}] =
             OtlpUtils.build_kv([{:ok, 1}, :invalid | :improper_tail])

    assert {:string_value, "[1 | 2]"} = OtlpUtils.to_kv_value([1 | 2])
  end

  test "encodes integers outside the protobuf int64 range as strings" do
    too_large = 9_223_372_036_854_775_808
    assert {:string_value, "9223372036854775808"} = OtlpUtils.to_kv_value(too_large)
  end

  test "normalizes attributes to flat string keys and JSON-safe values" do
    attributes = %{
      {:tuple, :key} => MapSet.new([:a]),
      nested: %{self() => [true, {:ok, <<255>>}]}
    }

    normalized = OtlpUtils.normalize_attributes(attributes)

    assert normalized[inspect({:tuple, :key})] =~ "MapSet"
    assert normalized["nested.#{inspect(self())}"] == [true, ["ok", "<<255>>"]]
  end

  test "normalize_value preserves JSON scalars and recursively sanitizes collections" do
    assert OtlpUtils.normalize_value(nil) == nil
    assert OtlpUtils.normalize_value(false) == false
    assert OtlpUtils.normalize_value(12.5) == 12.5

    normalized = OtlpUtils.normalize_value(%{self() => {:ok, MapSet.new([:a])}})

    assert ["ok", map_set_value] = normalized[inspect(self())]
    assert map_set_value =~ "MapSet"
  end
end
