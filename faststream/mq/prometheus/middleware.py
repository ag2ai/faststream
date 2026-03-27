from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from faststream._internal.constants import EMPTY
from faststream.mq.prometheus.provider import MQMetricsSettingsProvider
from faststream.mq.response import MQPublishCommand
from faststream.prometheus.middleware import PrometheusMiddleware

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class MQPrometheusMiddleware(
    PrometheusMiddleware[Any, MQPublishCommand],
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
            settings_provider_factory=lambda _: MQMetricsSettingsProvider(),
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_labels=custom_labels,
        )
