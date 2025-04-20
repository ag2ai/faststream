from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional

from faststream._internal.basic_types import AnyDict
from faststream._internal.constants import EMPTY
from faststream.prometheus.middleware import PrometheusMiddleware
from faststream.redis.prometheus.provider import settings_provider_factory
from faststream.redis.response import RedisPublishCommand

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class RedisPrometheusMiddleware(PrometheusMiddleware[RedisPublishCommand, AnyDict]):
    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Optional[Sequence[float]] = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=settings_provider_factory,
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
        )
