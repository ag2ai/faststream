---
# 0.5 - API
# 2 - Release
# 3 - Contributing
# 5 - Template Page
# 10 - Default
search:
  boost: 10
---

# Sentry Support

Unfortunately, **[Sentry](https://sentry.io/){.external-link target="_blank"}** does not fully support **OpenTelemetry**. Specifically, **Sentry** uses its own context format, which does not work well with **Python** implementations. The depth of cached spans is insufficient to properly bind to the `root` span.

## OpenTelemetry Collector

Fortunately, there is a workaround for exporting spans to **Sentry**. You just need to launch the `opentelemetry-collector` container, which will convert the traces to the **Sentry** format and export them according to the specified `DSN`.

1. Install [opentelemetry-exporter-otlp](https://pypi.org/project/opentelemetry-exporter-otlp/){.external-link target="_blank"} to export spans via **gRPC**:

    ```shell
    pip install opentelemetry-exporter-otlp
    ```

2. Setup the `TracerProvider` with **gRPC** exporter:

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

3. Create an `otel.yaml` configuration file with your **Sentry** `DSN`:

    ```yaml title="otel.yaml" linenums="1" hl_lines="9"
    receivers:
      otlp:
        protocols:
          grpc:
            endpoint: 0.0.0.0:4317

    exporters:
      sentry:
        dsn: "https://your-secret-dsn.com"

    service:
      pipelines:
        traces:
          receivers: [otlp]
          exporters: [sentry]
    ```

4. Run the **Docker** container with the `opentelemetry-collector`:

    ```yaml title="docker-compose.yaml" linenums="1""
    services:
      otel-collector:
        image: otel/opentelemetry-collector-contrib:latest
        command: [ "--config=/etc/otel.yaml" ]
        volumes:
          - ./otel.yaml:/etc/otel.yaml
        ports:
          - "4317:4317"
    ```

!!! note
    While this setup is somewhat limited due to the lack of full support from the `sentry-sdk`, your code remains independent of the tracing backend. You can switch to **Tempo** or **Jaeger** at any time by simply updating `otel.yaml`.

## Visualization

![HTML-page](../../../assets/img/sentry-trace.png){ .on-glb loading=lazy }
`Visualized via Sentry`

An example of a distributed trace visualization between two services in **Sentry**.

## Alternatives

If you're looking for a tracing and visualization system that is easy to configure and does not require additional infrastructure, consider [Logfire](https://logfire.pydantic.dev/docs/integrations/event-streams/faststream/){.external-link target="_blank"}.

![HTML-page](../../../assets/img/logfire-trace.png){ .on-glb loading=lazy }
`Visualized via Logfire`
