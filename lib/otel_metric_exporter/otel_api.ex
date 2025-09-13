defmodule OtelMetricExporter.OtelApi do
  @moduledoc false

  use Retry

  alias OtelMetricExporter.OtelApi.Config
  alias OtelMetricExporter.Protocol

  require Logger

  @schema NimbleOptions.new!(
            finch: [
              type: {:or, [:atom, :pid]},
              required: true,
              doc: "Finch process pid or registered name to use for sending requests."
            ],
            retry: [
              type: :boolean,
              default: true,
              doc: "Retry HTTP requests when receiving a transient error"
            ]
          )

  defstruct [:finch, :retry, :config, :scope]

  def public_options, do: Config.public_options()

  def new(opts, scope) do
    with {:ok, config, rest} <- Config.validate_for_scope(opts, scope),
         {own_opts, rest} <- Map.split(rest, [:finch, :retry]),
         {:ok, validated} <- NimbleOptions.validate(own_opts, @schema) do
      {:ok,
       %__MODULE__{
         config: config,
         scope: scope,
         finch: validated.finch,
         retry: validated.retry
       }, rest}
    end
  end

  def send_log_events(%__MODULE__{config: config} = api, events) do
    events
    |> apply_batch_limit(config.max_batch_size)
    |> Protocol.build_log_service_request(config.resource)
    |> send_proto("/v1/logs", api)
  end

  def send_metrics(%__MODULE__{config: config} = api, metrics) do
    metrics
    |> apply_batch_limit(config.max_batch_size)
    |> Protocol.build_metric_service_request(config.resource)
    |> send_proto("/v1/metrics", api)
  end

  @spec send_proto(struct(), String.t(), %__MODULE__{}) :: :ok | {:error, any()}
  defp send_proto(body, path, %__MODULE__{} = api) do
    body
    |> Protobuf.encode_to_iodata()
    |> build_finch_request(path, api)
    |> make_finch_request(api.finch, with_retry?: api.retry)
  end

  defp build_finch_request(body, path, %__MODULE__{} = api) do
    Finch.build(
      :post,
      url(api, path),
      Map.to_list(headers(api)),
      maybe_compress(body, api)
    )
  end

  defp make_finch_request(request, finch_pool, with_retry?: true) do
    retry with: exponential_backoff(1_000) |> randomize() |> expiry(20_000), atoms: [:retry] do
      case finch_request(request, finch_pool) do
        :ok ->
          :ok

        {:error, {:unexpected_status, %{status: status} = response} = reason}
        when status in [408, 429, 500, 502, 503, 504] ->
          Logger.warning(
            "Got transient error #{status} from server #{inspect(response)}, retrying...",
            request_path: request.path
          )

          {:retry, reason}

        {:error, {:unexpected_status, _response}} = permanent_error ->
          # This will be logged by the caller
          permanent_error

        {:error, reason} ->
          Logger.warning(
            "Got connection/transport error when sending metrics #{inspect(reason)}, retrying...",
            request_path: request.path
          )

          {:retry, reason}
      end
    else
      {:retry, reason} -> {:error, reason}
    end
  end

  defp make_finch_request(request, finch_pool, with_retry?: false) do
    finch_request(request, finch_pool)
  end

  defp finch_request(request, finch_pool) do
    request
    |> Finch.request(finch_pool)
    |> case do
      {:ok, %{status: 200}} ->
        :ok

      {:ok, response} ->
        {:error, {:unexpected_status, response}}

      {:error, _reason} = error ->
        error
    end
  end

  defp url(%__MODULE__{config: config}, path), do: config.otlp_endpoint <> path

  defp headers(%__MODULE__{config: %Config{otlp_compression: compression} = config}) do
    [:content_type, :accept, :compression]
    |> Enum.reduce(%{}, fn
      :content_type, acc -> Map.put(acc, "content-type", "application/x-protobuf")
      :accept, acc -> Map.put(acc, "accept", "application/x-protobuf")
      :compression, acc when compression == :gzip -> Map.put(acc, "content-encoding", "gzip")
      _, acc -> acc
    end)
    |> Map.merge(config.otlp_headers)
  end

  defp maybe_compress(body, %__MODULE__{config: %Config{otlp_compression: :gzip}}),
    do: :zlib.gzip(body)

  defp maybe_compress(body, _), do: body

  defp apply_batch_limit(batch, :infinity), do: batch
  defp apply_batch_limit(batch, limit), do: Enum.take(batch, limit)
end
