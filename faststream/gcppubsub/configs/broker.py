"""GCP Pub/Sub broker configuration."""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import BrokerConfig
from faststream.gcppubsub.configs.publisher import PublisherConfig
from faststream.gcppubsub.configs.retry import RetryConfig
from faststream.gcppubsub.configs.state import ConnectionState
from faststream.gcppubsub.configs.subscriber import SubscriberConfig

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

    # Grouped configuration objects
    publisher: PublisherConfig = field(default_factory=PublisherConfig)
    subscriber: SubscriberConfig = field(default_factory=SubscriberConfig)
    retry: RetryConfig = field(default_factory=RetryConfig)

    # Maintain backward compatibility properties
    @property
    def publisher_max_messages(self) -> int:
        """Publisher max messages (backward compatibility)."""
        return self.publisher.max_messages

    @property
    def publisher_max_bytes(self) -> int:
        """Publisher max bytes (backward compatibility)."""
        return self.publisher.max_bytes

    @property
    def publisher_max_latency(self) -> float:
        """Publisher max latency (backward compatibility)."""
        return self.publisher.max_latency

    @property
    def subscriber_max_messages(self) -> int:
        """Subscriber max messages (backward compatibility)."""
        return self.subscriber.max_messages

    @property
    def subscriber_ack_deadline(self) -> int:
        """Subscriber ack deadline (backward compatibility)."""
        return self.subscriber.ack_deadline

    @property
    def subscriber_max_extension(self) -> int:
        """Subscriber max extension (backward compatibility)."""
        return self.subscriber.max_extension

    @property
    def retry_max_attempts(self) -> int:
        """Retry max attempts (backward compatibility)."""
        return self.retry.max_attempts

    @property
    def retry_max_delay(self) -> float:
        """Retry max delay (backward compatibility)."""
        return self.retry.max_delay

    @property
    def retry_multiplier(self) -> float:
        """Retry multiplier (backward compatibility)."""
        return self.retry.multiplier

    @property
    def retry_min_delay(self) -> float:
        """Retry min delay (backward compatibility)."""
        return self.retry.min_delay
