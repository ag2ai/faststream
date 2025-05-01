---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Tracing

## Concept

Tracing is a form of observability that tracks the flow of requests as they move through various services in a distributed system. It provides insights into the interactions between services, highlighting performance bottlenecks and errors. The result of implementing tracing is a detailed map of the service interactions, often visualized as a trace diagram. This helps developers understand the behavior and performance of their applications. For an in-depth explanation, please refer to the [OpenTelemetry tracing specification](https://opentelemetry.io/docs/concepts/signals/traces/){.external-link target="_blank"}.

![HTML-page](../../../assets/img/simple-trace.png){ .on-glb loading=lazy }
`Visualized via Grafana and Tempo`

This trace is derived from the relationship between the following handlers:

```python linenums="1"
@broker.subscriber("first")
@broker.publisher("second")
async def first_handler(msg: str):
    await asyncio.sleep(0.1)
    return msg


@broker.subscriber("second")
@broker.publisher("third")
async def second_handler(msg: str):
    await asyncio.sleep(0.05)
    return msg


@broker.subscriber("third")
async def third_handler(msg: str):
    await asyncio.sleep(0.075)
```

## FastStream Tracing

**OpenTelemetry** tracing support in **FastStream** adheres to the [semantic conventions for messaging systems](https://opentelemetry.io/docs/specs/semconv/messaging/){.external-link target="_blank"}.

To enable tracing your broker:

1. Install `FastStream` with the `opentelemetry-sdk`:

    ```shell
    pip install faststream[otel]
    ```

2. Configure `TracerProvider`:

    ```python linenums="1" hl_lines="6"
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider

    resource = Resource.create(attributes={"service.name": "faststream"})
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    ```

3. Add `TelemetryMiddleware` to your broker:

=== "AIOKafka"
    ```python linenums="1" hl_lines="3 7"
    {!> docs_src/getting_started/opentelemetry/kafka_telemetry.py!}
    ```

=== "Confluent"
    ```python linenums="1" hl_lines="3 7"
    {!> docs_src/getting_started/opentelemetry/confluent_telemetry.py!}
    ```

=== "RabbitMQ"
    ```python linenums="1" hl_lines="3 7"
    {!> docs_src/getting_started/opentelemetry/rabbit_telemetry.py!}
    ```

=== "NATS"
    ```python linenums="1" hl_lines="3 7"
    {!> docs_src/getting_started/opentelemetry/nats_telemetry.py!}
    ```

=== "Redis"
    ```python linenums="1" hl_lines="3 7"
    {!> docs_src/getting_started/opentelemetry/redis_telemetry.py!}
    ```

## Exporting

To export traces, you must configure an exporter. Options include:

* [opentelemetry-exporter-jaeger](https://pypi.org/project/opentelemetry-exporter-jaeger/){.external-link target="_blank"} - for exporting to **Jaeger**
* [opentelemetry-exporter-otlp](https://pypi.org/project/opentelemetry-exporter-otlp/){.external-link target="_blank"} - for exporting via **gRPC** or **HTTP**
* `InMemorySpanExporter` from `opentelemetry.sdk.trace.export.in_memory_span_exporter` -  for local testing

There are other exporters also.

To configure the export of traces via `opentelemetry-exporter-otlp`:

1. Install the OTLP exporter `opentelemetry-exporter-otlp`:

    ```shell
    pip install opentelemetry-exporter-otlp
    ```

2. Configure the `OTLPSpanExporter`:

    ```python linenums="1"
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    exporter = OTLPSpanExporter(endpoint="http://127.0.0.1:4317")
    processor = BatchSpanProcessor(exporter)
    tracer_provider.add_span_processor(processor)
    ```

## Visualization

To visualize traces, send them to a backend system that supports distributed tracing, such as **Jaeger**, **Zipkin**, or **Grafana Tempo**. These systems provide user interfaces to visualize and analyze traces.

* **Jaeger**: Run **Jaeger** using Docker and configure your **OpenTelemetry** middleware to send traces to **Jaeger**. For more details, see the [Jaeger documentation](https://www.jaegertracing.io/){.external-link target="_blank"}.
* **Zipkin**: Like **Jaeger**, **Zipkin** can be run using **Docker** and configured with **OpenTelemetry** middleware. For more details, see the [Zipkin documentation](https://zipkin.io/){.external-link target="_blank"}.
* **Grafana Tempo**: **Grafana Tempo** is a high-scale, distributed tracing backend. Configure **OpenTelemetry** to export traces to **Tempo**, which can then be visualized using **Grafana**. For more details, see the [Grafana Tempo documentation](https://grafana.com/docs/tempo/latest/){.external-link target="_blank"}.

## Context Propagation

Quite often it is necessary to communicate with **other** services by propagating the trace context. To propagate the trace context, use the **CurrentSpan** object:

```python linenums="1" hl_lines="1-2 7 9-10 13"
from opentelemetry import trace, propagate
from faststream.opentelemetry import CurrentSpan

@broker.subscriber("symbol")
async def handler(
    msg: str,
    span: CurrentSpan,
) -> None:
    headers = {}
    propagate.inject(headers, context=trace.set_span_in_context(span))
    price = await exchange_client.get_symbol_price(
        msg,
        headers=headers,
    )
```
