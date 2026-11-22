import os

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)

from faststream.asgi import AsgiFastStream, AsgiResponse, get
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus import KafkaPrometheusMiddleware

registry = CollectorRegistry()

broker = KafkaBroker(
    middlewares=(
        KafkaPrometheusMiddleware(
            registry=registry,
            app_name="your-app-name",
        ),
    ),
)


@get
async def metrics(scope):
    if path := os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        # multi-process mode: collect metrics from all workers
        registry_ = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry_, path=path)
    else:
        # single-process mode: use the application registry
        registry_ = registry

    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    return AsgiResponse(generate_latest(registry_), status_code=200, headers=headers)


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", metrics),
    ],
)
