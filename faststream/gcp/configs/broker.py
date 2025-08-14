"""GCP Pub/Sub broker configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import BrokerConfig
from faststream.gcp.configs.publisher import PublisherConfig
from faststream.gcp.configs.retry import RetryConfig
from faststream.gcp.configs.state import ConnectionState
from faststream.gcp.configs.subscriber import SubscriberConfig

if TYPE_CHECKING:
    from aiohttp import ClientSession

    from faststream.gcp.publisher.producer import GCPFastProducer


@dataclass
class GCPBrokerConfig(BrokerConfig):
    """Configuration for GCP Pub/Sub broker."""

    producer: "GCPFastProducer"
    project_id: str
    connection: ConnectionState
    service_file: str | None = None
    emulator_host: str | None = None
    session: Optional["ClientSession"] = None

    # Grouped configuration objects
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    subscriber: SubscriberConfig = field(default_factory=SubscriberConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)
