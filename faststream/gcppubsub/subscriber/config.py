"""GCP Pub/Sub subscriber configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import (
    SubscriberSpecificationConfig,
    SubscriberUsecaseConfig,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubSubscriberSpecificationConfig(SubscriberSpecificationConfig):
    """GCP Pub/Sub subscriber specification configuration."""
    pass


@dataclass(kw_only=True)
class GCPPubSubSubscriberConfig(SubscriberUsecaseConfig):
    """GCP Pub/Sub subscriber configuration."""
    
    _outer_config: "GCPPubSubBrokerConfig"
    
    # GCP Pub/Sub specific options
    subscription: str
    topic: Optional[str] = None
    create_subscription: bool = True
    ack_deadline: Optional[int] = None
    max_messages: int = 10
    
    _no_ack: bool = field(default_factory=lambda: EMPTY, repr=False)
    
    def __post_init__(self):
        """Post-initialization setup."""
        # Set parser and decoder from broker config
        self.parser = self._outer_config.broker_parser
        self.decoder = self._outer_config.broker_decoder

    @property
    def ack_policy(self) -> AckPolicy:
        """Get acknowledgment policy."""
        if self._no_ack is not EMPTY and self._no_ack:
            return AckPolicy.MANUAL
        
        if self._ack_policy is EMPTY:
            return AckPolicy.ACK  # Automatically acknowledge messages after processing
        
        return self._ack_policy