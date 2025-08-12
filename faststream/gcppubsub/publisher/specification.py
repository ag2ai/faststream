"""GCP Pub/Sub publisher specifications."""

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.publisher.specification import PublisherSpecification
from faststream.gcppubsub.publisher.config import GCPPubSubPublisherSpecificationConfig

if TYPE_CHECKING:
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubPublisherSpecification(PublisherSpecification):
    """GCP Pub/Sub publisher specification."""

    def __init__(
        self,
        topic: str,
        _outer_config: "GCPPubSubBrokerConfig" = None,
        **kwargs: Any,
    ) -> None:
        """Initialize publisher specification.

        Args:
            topic: Topic name
            _outer_config: Broker configuration
            **kwargs: Additional options
        """
        self.topic = topic

        # Create specification config
        spec_config = GCPPubSubPublisherSpecificationConfig(
            title_=kwargs.get("title_"),
            description_=kwargs.get("description_"),
            schema_=kwargs.get("schema_"),
            **{
                k: v
                for k, v in kwargs.items()
                if k not in {"title_", "description_", "schema_"}
            },
        )

        super().__init__(
            _outer_config=_outer_config,
            specification_config=spec_config,
        )

    @property
    def call_name(self) -> str:
        """Get call name for logging."""
        return f"gcppubsub:{self.topic}"

    def get_log_context(
        self,
        message: Any,
        *,
        topic: str | None = None,
    ) -> dict[str, Any]:
        """Get logging context."""
        return {
            "topic": topic or self.topic,
        }
