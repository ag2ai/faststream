from faststream import FastStream
from faststream.nats import NatsBroker
from faststream.nats.prometheus import NatsPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = NatsBroker(
    middlewares=(
        NatsPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
