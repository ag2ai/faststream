from collections.abc import Sequence
from typing import TYPE_CHECKING, Optional, cast

from prometheus_client import Counter, Gauge, Histogram

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry
    from prometheus_client.registry import Collector


class MetricsContainer:
    __slots__ = (
        "_metrics_prefix",
        "_registry",
        "published_messages_duration_seconds",
        "published_messages_exceptions_total",
        "published_messages_total",
        "received_messages_in_process",
        "received_messages_size_bytes",
        "received_messages_total",
        "received_processed_messages_duration_seconds",
        "received_processed_messages_exceptions_total",
        "received_processed_messages_total",
    )

    DEFAULT_SIZE_BUCKETS = (
        2.0**4,
        2.0**6,
        2.0**8,
        2.0**10,
        2.0**12,
        2.0**14,
        2.0**16,
        2.0**18,
        2.0**20,
        2.0**22,
        2.0**24,
        float("inf"),
    )

    def __init__(
        self,
        registry: "CollectorRegistry",
        *,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
        custom_label_names: Sequence[str],
    ) -> None:
        self._registry = registry
        self._metrics_prefix = metrics_prefix

        received_messages_total_name = f"{metrics_prefix}_received_messages_total"
        self.received_messages_total = cast(
            "Counter",
            self._get_registered_metric(received_messages_total_name),
        ) or Counter(
            name=received_messages_total_name,
            documentation="Count of received messages by broker and handler",
            labelnames=["app_name", "broker", "handler", *custom_label_names],
            registry=registry,
        )

        received_messages_size_bytes_name = (
            f"{metrics_prefix}_received_messages_size_bytes"
        )
        self.received_messages_size_bytes = cast(
            "Histogram",
            self._get_registered_metric(received_messages_size_bytes_name),
        ) or Histogram(
            name=received_messages_size_bytes_name,
            documentation="Histogram of received messages size in bytes by broker and handler",
            labelnames=["app_name", "broker", "handler", *custom_label_names],
            registry=registry,
            buckets=received_messages_size_buckets or self.DEFAULT_SIZE_BUCKETS,
        )

        received_messages_in_process_name = (
            f"{metrics_prefix}_received_messages_in_process"
        )
        self.received_messages_in_process = cast(
            "Gauge",
            self._get_registered_metric(received_messages_in_process_name),
        ) or Gauge(
            name=received_messages_in_process_name,
            documentation="Gauge of received messages in process by broker and handler",
            labelnames=["app_name", "broker", "handler", *custom_label_names],
            registry=registry,
        )

        received_processed_messages_total_name = (
            f"{metrics_prefix}_received_processed_messages_total"
        )
        self.received_processed_messages_total = cast(
            "Counter",
            self._get_registered_metric(received_processed_messages_total_name),
        ) or Counter(
            name=received_processed_messages_total_name,
            documentation="Count of received processed messages by broker, handler and status",
            labelnames=["app_name", "broker", "handler", "status", *custom_label_names],
            registry=registry,
        )

        received_processed_messages_duration_seconds_name = (
            f"{metrics_prefix}_received_processed_messages_duration_seconds"
        )
        self.received_processed_messages_duration_seconds = cast(
            "Histogram",
            self._get_registered_metric(
                received_processed_messages_duration_seconds_name,
            ),
        ) or Histogram(
            name=received_processed_messages_duration_seconds_name,
            documentation="Histogram of received processed messages duration in seconds by broker and handler",
            labelnames=["app_name", "broker", "handler", *custom_label_names],
            registry=registry,
        )

        received_processed_messages_exceptions_total_name = (
            f"{metrics_prefix}_received_processed_messages_exceptions_total"
        )
        self.received_processed_messages_exceptions_total = cast(
            "Counter",
            self._get_registered_metric(
                received_processed_messages_exceptions_total_name,
            ),
        ) or Counter(
            name=received_processed_messages_exceptions_total_name,
            documentation="Count of received processed messages exceptions by broker, handler and exception_type",
            labelnames=[
                "app_name",
                "broker",
                "handler",
                "exception_type",
                *custom_label_names,
            ],
            registry=registry,
        )

        published_messages_total_name = f"{metrics_prefix}_published_messages_total"
        self.published_messages_total = cast(
            "Counter",
            self._get_registered_metric(published_messages_total_name),
        ) or Counter(
            name=published_messages_total_name,
            documentation="Count of published messages by destination and status",
            labelnames=[
                "app_name",
                "broker",
                "destination",
                "status",
                *custom_label_names,
            ],
            registry=registry,
        )

        published_messages_duration_seconds_name = (
            f"{metrics_prefix}_published_messages_duration_seconds"
        )
        self.published_messages_duration_seconds = cast(
            "Histogram",
            self._get_registered_metric(published_messages_duration_seconds_name),
        ) or Histogram(
            name=published_messages_duration_seconds_name,
            documentation="Histogram of published messages duration in seconds by broker and destination",
            labelnames=["app_name", "broker", "destination", *custom_label_names],
            registry=registry,
        )

        published_messages_exceptions_total_name = (
            f"{metrics_prefix}_published_messages_exceptions_total"
        )
        self.published_messages_exceptions_total = cast(
            "Counter",
            self._get_registered_metric(published_messages_exceptions_total_name),
        ) or Counter(
            name=published_messages_exceptions_total_name,
            documentation="Count of published messages exceptions by broker, destination and exception_type",
            labelnames=[
                "app_name",
                "broker",
                "destination",
                "exception_type",
                *custom_label_names,
            ],
            registry=registry,
        )

    def _get_registered_metric(self, metric_name: str) -> Optional["Collector"]:
        return self._registry._names_to_collectors.get(metric_name)
