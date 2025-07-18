from faststream import FastStream
from faststream.confluent import KafkaBroker
from faststream.confluent.opentelemetry import KafkaTelemetryMiddleware

broker = KafkaBroker(
    middlewares=(
        KafkaTelemetryMiddleware(tracer_provider=tracer_provider)
    ),
)
app = FastStream(broker)
