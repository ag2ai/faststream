from faststream import FastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = RabbitBroker(
    middlewares=(
        RabbitPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
