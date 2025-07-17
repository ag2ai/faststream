from faststream.asgi import AsgiFastStream
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus import KafkaPrometheusMiddleware
from prometheus_client import CollectorRegistry, make_asgi_app

registry = CollectorRegistry()

broker = KafkaBroker(
    middlewares=(
        KafkaPrometheusMiddleware(registry=registry),
    ),
)
app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ],
)
