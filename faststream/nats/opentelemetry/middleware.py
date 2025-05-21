from typing import Optional

from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import TracerProvider

from faststream.nats.opentelemetry.provider import telemetry_attributes_provider_factory
from faststream.nats.response import NatsPublishCommand
from faststream.opentelemetry.middleware import TelemetryMiddleware


class NatsTelemetryMiddleware(TelemetryMiddleware[NatsPublishCommand]):
    def __init__(
        self,
        *,
        tracer_provider: Optional[TracerProvider] = None,
        meter_provider: Optional[MeterProvider] = None,
        meter: Optional[Meter] = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=telemetry_attributes_provider_factory,
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=True,
        )
