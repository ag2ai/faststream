from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.opentelemetry import RedisTelemetryMiddleware

broker = RedisBroker(
    middlewares=(
        RedisTelemetryMiddleware(tracer_provider=tracer_provider),
    ),
)
app = FastStream(broker)
