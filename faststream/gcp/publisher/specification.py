"""GCP Pub/Sub publisher specifications."""

from typing import TYPE_CHECKING, Any

from faststream._internal.endpoint.publisher.specification import PublisherSpecification
from faststream.gcp.publisher.config import GCPPublisherSpecificationConfig

if TYPE_CHECKING:
    from faststream.gcp.configs.broker import GCPBrokerConfig


class GCPPublisherSpecification(PublisherSpecification):
    """GCP Pub/Sub publisher specification."""

    def __init__(
        self,
        topic: str,
        _outer_config: "GCPBrokerConfig | None" = None,
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
        spec_config = GCPPublisherSpecificationConfig(
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
            _outer_config=_outer_config,  # type: ignore[arg-type]
            specification_config=spec_config,
        )

    @property
    def call_name(self) -> str:
        """Get call name for logging."""
        topic_name = (
            f"{self._outer_config.prefix}{self.topic}"
            if self._outer_config
            else self.topic
        )
        return f"gcp:{topic_name}"

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
