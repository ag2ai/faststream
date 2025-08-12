"""GCP Pub/Sub subscriber specifications."""

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream.gcppubsub.subscriber.config import GCPPubSubSubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubSubscriberSpecification(SubscriberSpecification):
    """GCP Pub/Sub subscriber specification."""

    def __init__(
        self,
        subscription: str,
        topic: str | None = None,
        _outer_config: "GCPPubSubBrokerConfig | None" = None,
        calls: "CallsCollection[Any] | None" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize subscriber specification.

        Args:
            subscription: Subscription name
            topic: Topic name
            _outer_config: Broker configuration
            calls: Handler calls collection
            **kwargs: Additional options
        """
        self.subscription = subscription
        self.topic = topic

        # Create specification config
        spec_config = GCPPubSubSubscriberSpecificationConfig(
            title_=kwargs.get("title_"),
            description_=kwargs.get("description_"),
            **{k: v for k, v in kwargs.items() if k not in {"title_", "description_"}},
        )

        super().__init__(
            _outer_config=_outer_config,
            specification_config=spec_config,
            calls=calls,
        )

    @property
    def call_name(self) -> str:
        """Get call name for logging."""
        return f"gcppubsub:{self.subscription}"

    def get_log_context(
        self,
        message: Any,
        *,
        subscription: str | None = None,
        topic: str | None = None,
    ) -> dict[str, Any]:
        """Get logging context."""
        return {
            "subscription": subscription or self.subscription,
            "topic": topic or self.topic or "",
        }
