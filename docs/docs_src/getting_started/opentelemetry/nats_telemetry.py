from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.nats.opentelemetry import NatsTelemetryMiddleware

broker = NatsBroker(
    middlewares=(
        NatsTelemetryMiddleware(tracer_provider=tracer_provider),
    ),
)
app = FastStream(broker)
