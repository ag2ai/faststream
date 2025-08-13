"""GCP Pub/Sub subscriber specifications."""

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.subscriber.specification import SubscriberSpecification
from faststream.gcp.subscriber.config import GCPSubscriberSpecificationConfig
from faststream.specification.asyncapi.utils import resolve_payloads
from faststream.specification.schema import (
    Message,
    Operation,
    SubscriberSpec,
)
from faststream.specification.schema.bindings import (
    ChannelBinding,
    OperationBinding,
    gcp,
)

if TYPE_CHECKING:
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.gcp.configs.broker import GCPBrokerConfig


class GCPSubscriberSpecification(SubscriberSpecification):
    """GCP Pub/Sub subscriber specification."""

    def __init__(
        self,
        subscription: str,
        topic: str | None = None,
        _outer_config: "GCPBrokerConfig | None" = None,
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
        spec_config = GCPSubscriberSpecificationConfig(
            title_=kwargs.get("title_"),
            description_=kwargs.get("description_"),
            **{k: v for k, v in kwargs.items() if k not in {"title_", "description_"}},
        )

        # Provide defaults for None values
        from faststream._internal.endpoint.subscriber.call_item import CallsCollection

        default_calls = calls or CallsCollection()

        super().__init__(
            _outer_config=_outer_config,  # type: ignore[arg-type]
            specification_config=spec_config,
            calls=default_calls,
        )

    @property
    def call_name(self) -> str:
        """Get call name for logging."""
        subscription_name = (
            f"{self._outer_config.prefix}{self.subscription}"
            if self._outer_config
            else self.subscription
        )
        return f"gcp:{subscription_name}"

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

    @property
    def name(self) -> str:
        """Get subscriber name."""
        return (
            f"{self._outer_config.prefix}{self.subscription}"
            if self._outer_config
            else self.subscription
        )

    def get_schema(self) -> dict[str, SubscriberSpec]:
        """Get subscriber schema for AsyncAPI specification."""
        payloads = self.get_payloads()

        # Create bindings for GCP Pub/Sub
        channel_binding = gcp.ChannelBinding(
            topic=self.topic or self.subscription,
            subscription=self.subscription,
            project_id=getattr(self._outer_config, "project_id", None),
        )

        operation_binding = gcp.OperationBinding(
            ack_deadline=getattr(self.config, "ack_deadline", None),
            ordering_key=getattr(self.config, "ordering_key", None),
        )

        channel_name = self.name

        return {
            channel_name: SubscriberSpec(
                description=self.description,
                operation=Operation(
                    bindings=OperationBinding(
                        gcp=operation_binding,
                    ),
                    message=Message(
                        title=f"{channel_name}:Message",
                        payload=resolve_payloads(payloads),
                    ),
                ),
                bindings=ChannelBinding(
                    gcp=channel_binding,
                ),
            ),
        }
