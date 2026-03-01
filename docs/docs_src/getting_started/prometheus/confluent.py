from faststream import FastStream
from faststream.confluent import KafkaBroker
from faststream.confluent.prometheus import KafkaPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = KafkaBroker(
    middlewares=(
        KafkaPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
