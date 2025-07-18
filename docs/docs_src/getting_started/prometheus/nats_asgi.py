from faststream.asgi import AsgiFastStream
from faststream.nats import NatsBroker
from faststream.nats.prometheus import NatsPrometheusMiddleware
from prometheus_client import CollectorRegistry, make_asgi_app

registry = CollectorRegistry()

broker = NatsBroker(
    middlewares=(
        NatsPrometheusMiddleware(registry=registry),
    ),
)
app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ],
)
