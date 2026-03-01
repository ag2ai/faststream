from faststream.asgi import AsgiFastStream
from faststream.rabbit import RabbitBroker
from faststream.rabbit.prometheus import RabbitPrometheusMiddleware
from prometheus_client import CollectorRegistry, make_asgi_app

registry = CollectorRegistry()

broker = RabbitBroker(
    middlewares=(
        RabbitPrometheusMiddleware(registry=registry),
    ),
)
app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ],
)
