from faststream import FastStream
from faststream.redis import RedisBroker
from faststream.redis.prometheus import RedisPrometheusMiddleware
from prometheus_client import CollectorRegistry

registry = CollectorRegistry()

broker = RedisBroker(
    middlewares=(
        RedisPrometheusMiddleware(registry=registry),
    ),
)
app = FastStream(broker)
