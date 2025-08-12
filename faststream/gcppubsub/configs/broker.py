"""GCP Pub/Sub broker configuration."""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import BrokerConfig
from faststream.gcppubsub.configs.state import ConnectionState

if TYPE_CHECKING:
    from aiohttp import ClientSession

    from faststream.gcppubsub.publisher.producer import GCPPubSubFastProducer


@dataclass
class GCPPubSubBrokerConfig(BrokerConfig):
    """Configuration for GCP Pub/Sub broker."""

    producer: "GCPPubSubFastProducer"
    project_id: str
    connection: ConnectionState
    service_file: str | None = None
    emulator_host: str | None = None
    session: Optional["ClientSession"] = None

    # Publisher settings
    publisher_max_messages: int = 100
    publisher_max_bytes: int = 1024 * 1024  # 1MB
    publisher_max_latency: float = 0.01  # 10ms

    # Subscriber settings
    subscriber_max_messages: int = 1000
    subscriber_ack_deadline: int = 600  # 10 minutes
    subscriber_max_extension: int = 600  # 10 minutes

    # Retry settings
    retry_max_attempts: int = 5
    retry_max_delay: float = 60.0
    retry_multiplier: float = 2.0
    retry_min_delay: float = 1.0
