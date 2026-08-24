from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Union

from faststream._internal.config_value import Configurable
from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.nats.configs import NatsBrokerConfig

if TYPE_CHECKING:
    from faststream.nats.schemas import JStream


@dataclass(kw_only=True)
class NatsPublisherSpecificationConfig(PublisherSpecificationConfig):
    subject: Configurable[str]


@dataclass(kw_only=True)
class NatsPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "NatsBrokerConfig" = field(default_factory=NatsBrokerConfig)

    subject: Configurable[str]
    reply_to: Configurable[str]
    headers: dict[str, str] | None
    stream: Configurable[Union[str, "JStream"]] | None
    timeout: float | None
