from prometheus_client import CollectorRegistry, Counter
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from faststream import FastStream
from faststream.kafka import KafkaBroker

registry = CollectorRegistry()
CUSTOM_COUNTER = Counter("custom_counter", "Custom description", registry=registry)

broker = KafkaBroker(
    middlewares=(
        KafkaPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)


@broker.subscriber("test-topic")
async def inc_counter(count: int) -> None:
    CUSTOM_COUNTER.inc(amount=count)
