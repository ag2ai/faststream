from faststream import FastStream
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = KafkaBroker(
    middlewares=(
        KafkaPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
