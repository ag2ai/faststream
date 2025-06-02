from typing import TYPE_CHECKING, Optional, Sequence

from faststream.prometheus.middleware import BasePrometheusMiddleware
from faststream.redis.prometheus.provider import settings_provider_factory
from faststream.types import EMPTY

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class RedisPrometheusMiddleware(BasePrometheusMiddleware):
    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Optional[Sequence[float]] = None,
        multiprocess: bool = False,
        multiprocess_dir: Optional[str] = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=settings_provider_factory,
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            multiprocess=multiprocess,
            multiprocess_dir=multiprocess_dir,
        )
