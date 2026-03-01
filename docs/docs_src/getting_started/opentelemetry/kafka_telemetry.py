from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.kafka.opentelemetry import KafkaTelemetryMiddleware

broker = KafkaBroker(
    middlewares=(
        KafkaTelemetryMiddleware(tracer_provider=tracer_provider),
    ),
)
app = FastStream(broker)
