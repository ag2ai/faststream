from typing import TYPE_CHECKING, Optional, Sequence

from faststream.nats.prometheus.provider import settings_provider_factory
from faststream.prometheus.middleware import BasePrometheusMiddleware
from faststream.types import EMPTY

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class NatsPrometheusMiddleware(BasePrometheusMiddleware):
    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Optional[Sequence[float]] = None,
        is_multiprocess: bool = False,
        multiprocess_dir: str = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=settings_provider_factory,
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            is_multiprocess=is_multiprocess,
            multiprocess_dir=multiprocess_dir,
        )
