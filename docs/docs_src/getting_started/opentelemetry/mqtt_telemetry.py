from faststream import FastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.opentelemetry import MQTTTelemetryMiddleware

broker = MQTTBroker(
    middlewares=(
        MQTTTelemetryMiddleware(tracer_provider=tracer_provider),
    ),
)
app = FastStream(broker)
