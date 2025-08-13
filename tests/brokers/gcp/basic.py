"""Base test configurations for GCP Pub/Sub tests."""

from typing import Any

from faststream.gcp import GCPBroker, GCPRouter, TestGCPBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class GCPTestcaseConfig(BaseTestcaseConfig):
    """Base configuration for GCP Pub/Sub tests with real broker."""

    timeout: float = 5.0  # GCP Pub/Sub may need longer timeouts

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> GCPBroker:
        """Create GCP Pub/Sub broker instance."""
        # Note: GCPBroker may not support apply_types parameter
        broker_kwargs = {"project_id": "test-project"}
        broker_kwargs.update(kwargs)

        # Only add apply_types if the broker supports it
        try:
            return GCPBroker(apply_types=apply_types, **broker_kwargs)
        except TypeError:
            return GCPBroker(**broker_kwargs)

    def patch_broker(self, broker: GCPBroker, **kwargs: Any) -> GCPBroker:
        """Return broker as-is for real testing."""
        return broker

    def get_router(self, **kwargs: Any) -> GCPRouter:
        """Create GCP Pub/Sub router instance."""
        return GCPRouter(**kwargs)

    def get_subscriber_params(
        self,
        subscription: str,
        **kwargs: Any,
    ) -> tuple[tuple[Any, ...], dict[str, Any]]:
        """Get subscriber parameters for GCP Pub/Sub.

        Args:
            subscription: Subscription name (also used as topic name for compatibility)
            **kwargs: Additional subscriber parameters

        Returns:
            Tuple of (args, kwargs) for subscriber creation
        """
        # For GCP Pub/Sub base test compatibility, use subscription name as topic name
        # This allows base tests to publish to the queue name and have it routed correctly
        kwargs.setdefault("topic", subscription)
        args = (subscription,)
        return args, kwargs


class GCPMemoryTestcaseConfig(GCPTestcaseConfig):
    """Configuration for in-memory testing using TestGCPBroker."""

    def patch_broker(self, broker: GCPBroker, **kwargs: Any) -> GCPBroker:
        """Wrap broker with test client for in-memory testing."""
        return TestGCPBroker(broker, **kwargs)
