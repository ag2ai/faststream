"""GCP Pub/Sub OpenTelemetry middleware."""

from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import TracerProvider

from faststream.gcp.opentelemetry.provider import telemetry_attributes_provider_factory
from faststream.gcp.response import GCPPublishCommand
from faststream.opentelemetry.middleware import TelemetryMiddleware


class GCPTelemetryMiddleware(TelemetryMiddleware[GCPPublishCommand]):
    """GCP Pub/Sub OpenTelemetry middleware for trace context propagation.

    This middleware provides:
    - Automatic trace context injection into published messages
    - Trace context extraction from consumed messages
    - Span creation for publish and consume operations
    - GCP Pub/Sub specific span attributes and metrics

    Example:
        ```python
        from faststream.gcp import GCPBroker
        from faststream.gcp.opentelemetry import GCPTelemetryMiddleware

        broker = GCPBroker(
            project_id="my-project",
            middlewares=[GCPTelemetryMiddleware()]
        )
        ```
    """

    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        meter: Meter | None = None,
    ) -> None:
        """Initialize GCP Pub/Sub telemetry middleware.

        Args:
            tracer_provider: OpenTelemetry tracer provider for creating tracers
            meter_provider: OpenTelemetry meter provider for creating meters
            meter: OpenTelemetry meter for creating instruments
        """
        super().__init__(
            settings_provider_factory=telemetry_attributes_provider_factory,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=True,  # Enable message counting for GCP
        )
