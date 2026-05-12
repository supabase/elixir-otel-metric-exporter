defmodule OtelMetricExporter.OtlpUtils do
  @moduledoc false

  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{
    AnyValue,
    KeyValue,
    KeyValueList,
    ArrayValue
  }

  def build_kv(tags, key_prefix \\ "")

  def build_kv(tags, key_prefix) when is_map(tags) or is_list(tags) do
    Enum.flat_map(tags, fn {key, value} ->
      key = key_prefix <> key_to_string(key)

      cond do
        is_struct(value) ->
          build_key_value(key, value)

        is_map(value) ->
          build_kv(value, key <> ".")

        true ->
          build_key_value(key, value)
      end
    end)
  end

  def build_kv(_tags, _key_prefix), do: []

  def to_kv_value(value) when is_binary(value), do: {:string_value, value}
  def to_kv_value(value) when is_atom(value), do: {:string_value, to_string(value)}
  def to_kv_value(value) when is_integer(value), do: {:int_value, value}
  def to_kv_value(value) when is_float(value), do: {:double_value, value}
  def to_kv_value(value) when is_boolean(value), do: {:bool_value, value}
  def to_kv_value(value) when is_struct(value), do: {:string_value, safe_to_string(value)}

  def to_kv_value([{k, _} | _] = value) when is_atom(k),
    do: {:kvlist_value, %KeyValueList{values: build_kv(value)}}

  def to_kv_value(value) when is_list(value),
    do: {:array_value, %ArrayValue{values: Enum.map(value, &%AnyValue{value: to_kv_value(&1)})}}

  def to_kv_value(value) when is_tuple(value), do: to_kv_value(Tuple.to_list(value))
  def to_kv_value(value) when is_pid(value), do: to_kv_value(inspect(value))
  def to_kv_value(any), do: to_kv_value(inspect(any))

  defp build_key_value(key, value) do
    [
      %KeyValue{
        key: key,
        value: %AnyValue{value: to_kv_value(value)}
      }
    ]
  end

  defp key_to_string(key), do: safe_to_string(key)

  defp safe_to_string(value) do
    to_string(value)
  rescue
    Protocol.UndefinedError -> inspect(value)
  end
end
