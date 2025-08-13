"""GCP Pub/Sub publisher configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)

if TYPE_CHECKING:
    from faststream.gcp.configs.broker import GCPBrokerConfig


class GCPPublisherSpecificationConfig(PublisherSpecificationConfig):
    """GCP Pub/Sub publisher specification configuration."""


@dataclass(kw_only=True)
class GCPPublisherConfig(PublisherUsecaseConfig):
    """GCP Pub/Sub publisher configuration."""

    _outer_config: "GCPBrokerConfig"

    # GCP Pub/Sub specific options
    topic: str
    create_topic: bool = True
    ordering_key: str | None = None
