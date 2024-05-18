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

**OpenTelemetry** is an open-source observability framework designed to provide a unified standard for collecting and exporting telemetry data such as traces, metrics, and logs. It aims to make observability a built-in feature of software development, simplifying the integration and standardization of telemetry data across various services. For more details, you can read the official [OpenTelemetry documentation](https://opentelemetry.io/).

## Tracing

Tracing is a form of observability that tracks the flow of requests as they move through various services in a distributed system. It provides insights into the interactions between services, highlighting performance bottlenecks and errors. The result of implementing tracing is a detailed map of the service interactions, often visualized as a trace diagram. This helps developers understand the behavior and performance of their applications. For an in-depth explanation, refer to the [OpenTelemetry tracing specification](https://opentelemetry.io/docs/concepts/signals/traces/).

![HTML-page](../../../assets/img/simple-trace.png){ loading=lazy }


## FastStream Tracing

**OpenTelemetry** tracing support in **FastStream** adheres to the [semantic conventions for messaging systems](https://opentelemetry.io/docs/specs/semconv/messaging/).

Just add `TelemetryMiddleware` to your broker:

{! includes/getting_started/opentelemetry/1.md !}

### Exporting

To export traces, you must select and configure an exporter yourself:

* [opentelemetry-exporter-jaeger](https://pypi.org/project/opentelemetry-exporter-jaeger/) to export to **Jaeger**
* [opentelemetry-exporter-otlp](https://pypi.org/project/opentelemetry-exporter-otlp/) for export via **gRPC** or **HTTP**
* ``InMemorySpanExporter`` from ``opentelemetry.sdk.trace.export.in_memory_span_exporter`` for local tests

There are other exporters.

Configuring the export of traces via `opentelemetry-exporter-otlp`:

```python linenums="1" hl_lines="10-12"
from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.grpc.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor

resource = Resource.create(attributes={"service.name": "faststream"})
tracer_provider = TracerProvider(resource=resource)
trace.set_tracer_provider(tracer_provider)
exporter = OTLPSpanExporter(endpoint="http://127.0.0.1:4317")
processor = BatchSpanProcessor(exporter)
tracer_provider.add_span_processor(processor)
```

### Visualization

To visualize traces, you can send them to a backend system that supports distributed tracing, such as **Jaeger**, **Zipkin**, or **Grafana Tempo**. These systems provide a user interface to visualize and analyze traces.

* **Jaeger**: You can run **Jaeger** using Docker and configure your **OpenTelemetry** middleware to send traces to **Jaeger**. For more details, see the [Jaeger documentation](https://www.jaegertracing.io/).
* **Zipkin**: Similar to **Jaeger**, you can run **Zipkin** using **Docker** and configure the **OpenTelemetry** middleware accordingly. For more details, see the [Zipkin documentation](https://zipkin.io/).
* **Grafana Tempo**: **Grafana Tempo** is a high-scale distributed tracing backend. You can configure **OpenTelemetry** to export traces to **Tempo**, which can then be visualized using **Grafana**. For more details, see the [Grafana Tempo documentation](https://grafana.com/docs/tempo/latest/).

## Example

To see how to set up, visualize, and configure tracing for **FastStream** services, go to [example](https://github.com/draincoder/faststream-monitoring).

![HTML-page](../../../assets/img/distributed-trace.png){ loading=lazy }
