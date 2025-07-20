from faststream.asgi import AsgiFastStream
from faststream.redis import RedisBroker
from faststream.redis.prometheus import RedisPrometheusMiddleware
from prometheus_client import CollectorRegistry, make_asgi_app

registry = CollectorRegistry()

broker = RedisBroker(
    middlewares=(
        RedisPrometheusMiddleware(registry=registry),
    ),
)
app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", make_asgi_app(registry)),
    ],
)
