from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from aio_pika import IncomingMessage

from faststream._internal.constants import EMPTY
from faststream.prometheus.middleware import PrometheusMiddleware
from faststream.rabbit.prometheus.provider import RabbitMetricsSettingsProvider
from faststream.rabbit.response import RabbitPublishCommand

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class RabbitPrometheusMiddleware(
    PrometheusMiddleware[IncomingMessage, RabbitPublishCommand],
):
    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
        custom_labels: dict[str, str | Callable[[Any], str]] | None = None,
    ) -> None:
        super().__init__(
            settings_provider_factory=lambda _: RabbitMetricsSettingsProvider(),
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_labels=custom_labels,
        )
