import os

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    generate_latest,
    multiprocess,
)

from faststream.asgi import AsgiFastStream, AsgiResponse
from faststream.kafka import KafkaBroker
from faststream.kafka.prometheus import KafkaPrometheusMiddleware

# Create registry for metrics
registry = CollectorRegistry()

# Create broker with Prometheus middleware
broker = KafkaBroker(
    middlewares=[
        KafkaPrometheusMiddleware(
            registry=registry,
            app_name="multiprocess-test-app",
        )
    ]
)


@broker.subscriber("test-multiprocess-queue")
async def handle_message(msg: str) -> None:
    """Handle incoming messages."""


async def metrics_endpoint(scope, receive, send):
    """Metrics endpoint that supports multi-process mode."""
    if path := os.environ.get("PROMETHEUS_MULTIPROC_DIR"):
        # Multi-process mode: aggregate from all workers
        registry_ = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry_, path=path)
    else:
        # Single process mode: use the default registry
        registry_ = registry

    headers = {"Content-Type": CONTENT_TYPE_LATEST}
    response = AsgiResponse(generate_latest(registry_), status_code=200, headers=headers)
    await response(scope, receive, send)


app = AsgiFastStream(
    broker,
    asgi_routes=[
        ("/metrics", metrics_endpoint),
    ],
)
