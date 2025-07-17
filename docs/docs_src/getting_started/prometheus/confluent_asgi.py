from faststream.asgi import AsgiFastStream
from faststream.confluent import KafkaBroker
from faststream.confluent.prometheus import KafkaPrometheusMiddleware
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
