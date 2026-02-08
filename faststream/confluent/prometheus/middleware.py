from collections.abc import Callable, Sequence
from typing import TYPE_CHECKING, Any

from confluent_kafka import Message

from faststream._internal.constants import EMPTY
from faststream.confluent.prometheus.provider import settings_provider_factory
from faststream.confluent.response import KafkaPublishCommand
from faststream.prometheus.middleware import PrometheusMiddleware

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class KafkaPrometheusMiddleware(
    PrometheusMiddleware[
        Message | Sequence[Message],
        KafkaPublishCommand,
    ],
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
            settings_provider_factory=settings_provider_factory,  # type: ignore[arg-type]
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
            custom_labels=custom_labels,
        )
