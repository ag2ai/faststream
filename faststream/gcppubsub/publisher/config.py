"""GCP Pub/Sub publisher configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

if TYPE_CHECKING:
    from faststream.gcppubsub.configs.broker import GCPPubSubBrokerConfig


class GCPPubSubPublisherSpecificationConfig(PublisherSpecificationConfig):
    """GCP Pub/Sub publisher specification configuration."""
    pass


@dataclass(kw_only=True)
class GCPPubSubPublisherConfig(PublisherUsecaseConfig):
    """GCP Pub/Sub publisher configuration."""
    
    _outer_config: "GCPPubSubBrokerConfig"
    
    # GCP Pub/Sub specific options
    topic: str
    create_topic: bool = True
    ordering_key: Optional[str] = None