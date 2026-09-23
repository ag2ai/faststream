from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import TracerProvider

from faststream.opentelemetry.middleware import TelemetryMiddleware
from faststream.rabbit.opentelemetry.provider import RabbitTelemetrySettingsProvider
from faststream.rabbit.response import RabbitPublishCommand


class RabbitTelemetryMiddleware(TelemetryMiddleware[RabbitPublishCommand]):
    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        meter: Meter | None = None,
        include_messages_counters: bool = False,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: RabbitTelemetrySettingsProvider(),
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=include_messages_counters,
        )
