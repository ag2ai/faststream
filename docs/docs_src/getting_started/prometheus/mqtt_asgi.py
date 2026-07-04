from faststream.asgi import AsgiFastStream
from faststream.mqtt import MQTTBroker
from faststream.mqtt.prometheus import MQTTPrometheusMiddleware
from prometheus_client import CollectorRegistry, make_asgi_app

registry = CollectorRegistry()

broker = MQTTBroker(
    middlewares=(
        MQTTPrometheusMiddleware(registry=registry),
    ),
)
app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ],
)
