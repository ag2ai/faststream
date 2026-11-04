from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream._internal.utils.path import Address
from faststream.rabbit.configs import RabbitBrokerConfig

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue

    from .options import PublishKwargs


@dataclass(kw_only=True, slots=True)
class RabbitPublisherSpecificationConfig(PublisherSpecificationConfig):
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    routing_address: Address
    message_kwargs: "PublishKwargs"


@dataclass(kw_only=True, slots=True)
class RabbitPublisherConfig(PublisherUsecaseConfig):
    _outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    routing_address: Address
    message_kwargs: "PublishKwargs"
