"""Base test configurations for GCP Pub/Sub tests."""

from typing import Any

from faststream.gcppubsub import GCPPubSubBroker, GCPPubSubRouter, TestGCPPubSubBroker
from tests.brokers.base.basic import BaseTestcaseConfig


class GCPPubSubTestcaseConfig(BaseTestcaseConfig):
    """Base configuration for GCP Pub/Sub tests with real broker."""

    timeout: float = 5.0  # GCP Pub/Sub may need longer timeouts

    def get_broker(
        self,
        apply_types: bool = False,
        **kwargs: Any,
    ) -> GCPPubSubBroker:
        """Create GCP Pub/Sub broker instance."""
        # Note: GCPPubSubBroker may not support apply_types parameter
        broker_kwargs = {"project_id": "test-project"}
        broker_kwargs.update(kwargs)

        # Only add apply_types if the broker supports it
        try:
            return GCPPubSubBroker(apply_types=apply_types, **broker_kwargs)
        except TypeError:
            return GCPPubSubBroker(**broker_kwargs)

    def patch_broker(self, broker: GCPPubSubBroker, **kwargs: Any) -> GCPPubSubBroker:
        """Return broker as-is for real testing."""
        return broker

    def get_router(self, **kwargs: Any) -> GCPPubSubRouter:
        """Create GCP Pub/Sub router instance."""
        return GCPPubSubRouter(**kwargs)

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


class GCPPubSubMemoryTestcaseConfig(GCPPubSubTestcaseConfig):
    """Configuration for in-memory testing using TestGCPPubSubBroker."""

    def patch_broker(self, broker: GCPPubSubBroker, **kwargs: Any) -> GCPPubSubBroker:
        """Wrap broker with test client for in-memory testing."""
        return TestGCPPubSubBroker(broker, **kwargs)
