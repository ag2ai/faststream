from opentelemetry.metrics import Meter, MeterProvider
from opentelemetry.trace import TracerProvider

from faststream.mqtt.opentelemetry.provider import MQTTTelemetrySettingsProvider
from faststream.mqtt.response import MQTTPublishCommand
from faststream.opentelemetry.middleware import TelemetryMiddleware


class MQTTTelemetryMiddleware(TelemetryMiddleware[MQTTPublishCommand]):
    def __init__(
        self,
        *,
        tracer_provider: TracerProvider | None = None,
        meter_provider: MeterProvider | None = None,
        meter: Meter | None = None,
        include_messages_counters: bool = True,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: MQTTTelemetrySettingsProvider(),
            tracer_provider=tracer_provider,
            meter_provider=meter_provider,
            meter=meter,
            include_messages_counters=include_messages_counters,
        )
