"""GCP Pub/Sub subscriber configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, cast

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.gcp.parser import GCPParser
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream._internal.types import AsyncCallable
    from faststream.gcp.configs.broker import GCPBrokerConfig


class GCPSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    """GCP Pub/Sub subscriber specification configuration."""


@dataclass(kw_only=True)
class GCPSubscriberConfig(SubscriberUsecaseConfig):
    """GCP Pub/Sub subscriber configuration."""

    _outer_config: "GCPBrokerConfig"

    # GCP Pub/Sub specific options
    subscription: str
    topic: str | None = None
    create_subscription: bool = True
    ack_deadline: int | None = None
    max_messages: int = 10

    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)

    def __post_init__(self) -> None:
        """Post-initialization setup."""
        # Set parser and decoder to defaults - broker parser will be composed separately
        default_parser = GCPParser()
        self.parser = cast("AsyncCallable", default_parser.parse_message)
        self.decoder = cast("AsyncCallable", default_parser.decode_message)

    @property
    def ack_policy(self) -> AckPolicy:
        """Get acknowledgment policy."""
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.MANUAL

        if self._ack_policy is EMPTY:
            return AckPolicy.ACK  # Automatically acknowledge messages after processing

        return self._ack_policy
