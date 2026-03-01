---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# OpenTelemetry

**OpenTelemetry** is an open-source observability framework designed to provide a unified standard for collecting and exporting telemetry data such as traces, metrics, and logs. It aims to make observability a built-in feature of software development, simplifying the integration and standardization of telemetry data across various services. For more details, you can read the official [OpenTelemetry documentation](https://opentelemetry.io/){.external-link target="_blank"}.

## Quick Start

To use **OpenTelemetry** in **FastStream**, you just need to:

1. Install `FastStream` along with `opentelemetry-sdk` and `opentelemetry-exporter-otlp`

    ```shell
    pip install "faststream[otel]" opentelemetry-exporter-otlp
    ```

2. Configure `TracerProvider` with the **gRPC** exporter:

    ```python linenums="1" hl_lines="8 10"
    from opentelemetry import trace
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
    from opentelemetry.sdk.trace.export import BatchSpanProcessor

    resource = Resource.create(attributes={"service.name": "faststream"})
    tracer_provider = TracerProvider(resource=resource)
    trace.set_tracer_provider(tracer_provider)
    exporter = OTLPSpanExporter(endpoint="http://localhost:4317")
    processor = BatchSpanProcessor(exporter)
    tracer_provider.add_span_processor(processor)
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

In the following documentation sections, you can find detailed information about all **OpenTelemetry** features available in **FastStream**.

## OpenTelemetry FastStream Example

You can also take a look at a pre-configured project and use it as a reference for your services and infrastructure.

To see how to set up, visualize, and configure tracing for **FastStream** services, checkout the [example project](https://github.com/draincoder/faststream-monitoring){.external-link target="_blank"}.

The example includes:

* Three `FastStream` services
* Exporting traces to `Grafana Tempo` via `gRPC`
* Visualization of traces using `Grafana`
* Collecting metrics and exporting them with `Prometheus`
* A `Grafana dashboard` for metrics
* Examples with custom spans
* A pre-configured `docker-compose` with the entire infrastructure

![HTML-page](../../../assets/img/distributed-trace.png){ .on-glb loading=lazy }
`Visualized via Grafana and Tempo`
