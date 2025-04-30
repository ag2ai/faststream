from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

from faststream._internal.publisher.configs import (
    PublisherUseCaseConfigs,
)

if TYPE_CHECKING:
    from faststream.nats.schemas import JStream


@dataclass
class NatsPublisherBaseConfigs(PublisherUseCaseConfigs):
    subject: str
    reply_to: str
    headers: Optional[dict[str, str]]
    stream: Optional["JStream"]
    timeout: Optional[float]
