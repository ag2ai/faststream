"""GCP Pub/Sub Prometheus middleware."""

from collections.abc import Sequence
from typing import TYPE_CHECKING

from gcloud.aio.pubsub import PubsubMessage

from faststream._internal.constants import EMPTY
from faststream.gcp.prometheus.provider import GCPMetricsSettingsProvider
from faststream.gcp.response import GCPPublishCommand
from faststream.prometheus.middleware import PrometheusMiddleware

if TYPE_CHECKING:
    from prometheus_client import CollectorRegistry


class GCPPrometheusMiddleware(PrometheusMiddleware[GCPPublishCommand, PubsubMessage]):
    """Prometheus middleware for GCP Pub/Sub broker."""

    def __init__(
        self,
        *,
        registry: "CollectorRegistry",
        app_name: str = EMPTY,
        metrics_prefix: str = "faststream",
        received_messages_size_buckets: Sequence[float] | None = None,
    ) -> None:
        """Initialize GCP Prometheus middleware.

        Args:
            registry: Prometheus metrics registry
            app_name: Application name for metrics
            metrics_prefix: Prefix for metric names
            received_messages_size_buckets: Histogram buckets for message size metrics
        """
        super().__init__(
            settings_provider_factory=lambda _: GCPMetricsSettingsProvider(),
            registry=registry,
            app_name=app_name,
            metrics_prefix=metrics_prefix,
            received_messages_size_buckets=received_messages_size_buckets,
        )
