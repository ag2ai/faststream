from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from prometheus_client import CollectorRegistry, start_http_server

registry = CollectorRegistry()

start_http_server(8001)


def setup_otel() -> TracerProvider:

    resource = Resource.create(
        attributes={
            "service.name": "faststream-worker",
            "service.version": "1.0.0",
            "deployment.environment": "production",
        }
    )

    tracer_provider = TracerProvider(resource=resource)

    otlp_exporter = OTLPSpanExporter(
        endpoint="http://localhost:4318/v1/traces",
    )

    batch_processor = BatchSpanProcessor(otlp_exporter)
    tracer_provider.add_span_processor(batch_processor)

    trace.set_tracer_provider(tracer_provider)

    return tracer_provider


tracer_provider = setup_otel()
