defmodule OtelMetricExporter.OtlpUtils do
  @moduledoc false

  alias OtelMetricExporter.Opentelemetry.Proto.Common.V1.{
    AnyValue,
    KeyValue,
    KeyValueList,
    ArrayValue
  }

  @int64_min -9_223_372_036_854_775_808
  @int64_max 9_223_372_036_854_775_807

  def build_kv(tags, key_prefix \\ "") do
    do_build_kv(tags, safe_to_string(key_prefix))
  end

  @doc false
  def normalize_attributes(tags, key_prefix \\ "") do
    do_normalize_attributes(tags, safe_to_string(key_prefix))
  end

  @doc false
  def normalize_value(value) when is_boolean(value) or is_number(value) or is_nil(value),
    do: value

  def normalize_value(value) when is_binary(value), do: safe_binary(value)
  def normalize_value(value) when is_atom(value), do: Atom.to_string(value)
  def normalize_value(value) when is_struct(value), do: safe_to_string(value)

  def normalize_value(value) when is_map(value) do
    Map.new(value, fn {key, nested_value} ->
      {safe_to_string(key), normalize_value(nested_value)}
    end)
  end

  def normalize_value(value) when is_list(value) do
    case list_items(value) do
      {:ok, items} -> Enum.map(items, &normalize_value/1)
      :improper -> safe_inspect(value)
    end
  end

  def normalize_value(value) when is_tuple(value) do
    value
    |> Tuple.to_list()
    |> Enum.map(&normalize_value/1)
  end

  def normalize_value(value), do: safe_inspect(value)

  def to_kv_value(value) when is_boolean(value), do: {:bool_value, value}
  def to_kv_value(value) when is_binary(value), do: {:string_value, safe_binary(value)}
  def to_kv_value(value) when is_atom(value), do: {:string_value, to_string(value)}

  def to_kv_value(value) when is_integer(value) and value >= @int64_min and value <= @int64_max,
    do: {:int_value, value}

  def to_kv_value(value) when is_integer(value), do: {:string_value, Integer.to_string(value)}
  def to_kv_value(value) when is_float(value), do: {:double_value, value}
  def to_kv_value(value) when is_struct(value), do: {:string_value, safe_to_string(value)}

  def to_kv_value(value) when is_map(value),
    do: {:kvlist_value, %KeyValueList{values: do_build_kv(value, "")}}

  def to_kv_value(value) when is_list(value) do
    case list_items(value) do
      {:ok, []} ->
        {:array_value, %ArrayValue{values: []}}

      {:ok, items} ->
        if Enum.all?(items, &match?({_, _}, &1)) do
          {:kvlist_value, %KeyValueList{values: do_build_kv(items, "")}}
        else
          {:array_value, %ArrayValue{values: Enum.map(items, &%AnyValue{value: to_kv_value(&1)})}}
        end

      :improper ->
        {:string_value, safe_inspect(value)}
    end
  end

  def to_kv_value(value) when is_tuple(value), do: to_kv_value(Tuple.to_list(value))
  def to_kv_value(value), do: {:string_value, safe_inspect(value)}

  defp do_build_kv(tags, key_prefix) do
    Enum.flat_map(attribute_pairs(tags), fn {key, value} ->
      key = key_prefix <> safe_to_string(key)

      cond do
        is_struct(value) ->
          build_key_value(key, value)

        is_map(value) ->
          do_build_kv(value, key <> ".")

        true ->
          build_key_value(key, value)
      end
    end)
  end

  defp do_normalize_attributes(tags, key_prefix) do
    Enum.reduce(attribute_pairs(tags), %{}, fn {key, value}, attributes ->
      key = key_prefix <> safe_to_string(key)

      if is_map(value) and not is_struct(value) do
        Map.merge(attributes, do_normalize_attributes(value, key <> "."))
      else
        Map.put(attributes, key, normalize_value(value))
      end
    end)
  end

  defp build_key_value(key, value) do
    [
      %KeyValue{
        key: key,
        value: %AnyValue{value: to_kv_value(value)}
      }
    ]
  end

  defp attribute_pairs(value) when is_struct(value), do: []
  defp attribute_pairs(value) when is_map(value), do: Map.to_list(value)

  defp attribute_pairs(value) when is_list(value),
    do: collect_attribute_pairs(value, [])

  defp attribute_pairs(_value), do: []

  defp collect_attribute_pairs([], pairs), do: Enum.reverse(pairs)

  defp collect_attribute_pairs([{key, value} | rest], pairs),
    do: collect_attribute_pairs(rest, [{key, value} | pairs])

  defp collect_attribute_pairs([_invalid | rest], pairs),
    do: collect_attribute_pairs(rest, pairs)

  defp collect_attribute_pairs(_improper_tail, pairs), do: Enum.reverse(pairs)

  defp list_items(value), do: list_items(value, [])
  defp list_items([], items), do: {:ok, Enum.reverse(items)}
  defp list_items([item | rest], items), do: list_items(rest, [item | items])
  defp list_items(_improper_tail, _items), do: :improper

  defp safe_binary(value) do
    if String.valid?(value), do: value, else: safe_inspect(value)
  end

  defp safe_to_string(value) do
    result =
      try do
        {:ok, to_string(value)}
      rescue
        _exception -> :error
      catch
        _kind, _reason -> :error
      end

    case result do
      {:ok, string} when is_binary(string) -> safe_binary(string)
      _error -> safe_inspect(value)
    end
  end

  defp safe_inspect(value) do
    case inspect_value(value, []) do
      {:ok, inspected} -> safe_inspected_binary(inspected)
      :error -> inspect_without_struct_protocol(value)
    end
  end

  defp inspect_without_struct_protocol(value) do
    case inspect_value(value, structs: false) do
      {:ok, inspected} -> safe_inspected_binary(inspected)
      :error -> "<unprintable>"
    end
  end

  defp inspect_value(value, opts) do
    try do
      {:ok, inspect(value, opts)}
    rescue
      _exception -> :error
    catch
      _kind, _reason -> :error
    end
  end

  defp safe_inspected_binary(value) when is_binary(value) do
    if String.valid?(value), do: value, else: "<unprintable>"
  end

  defp safe_inspected_binary(_value), do: "<unprintable>"
end
