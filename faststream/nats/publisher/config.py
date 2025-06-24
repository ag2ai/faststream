from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Optional

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.nats.configs import NatsBrokerConfig

if TYPE_CHECKING:
    from faststream.nats.schemas import JStream


@dataclass(kw_only=True)
class NatsPublisherSpecificationConfig(PublisherSpecificationConfig):
    subject: str


@dataclass(kw_only=True)
class NatsPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: str
    reply_to: str
    headers: dict[str, str] | None
    stream: Optional["JStream"]
    timeout: float | None
