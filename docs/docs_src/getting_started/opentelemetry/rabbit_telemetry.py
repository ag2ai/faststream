from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.opentelemetry import RabbitTelemetryMiddleware

broker = RabbitBroker(
    middlewares=(
        RabbitTelemetryMiddleware(tracer_provider=tracer_provider),
    ),
)
app = FastStream(broker)
