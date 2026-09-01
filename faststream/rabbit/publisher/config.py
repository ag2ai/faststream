from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream._internal.utils.path import Address
from faststream.rabbit.configs import RabbitBrokerConfig, RabbitConfig

if TYPE_CHECKING:
    from .options import PublishKwargs


@dataclass(kw_only=True)
class RabbitPublisherSpecificationConfig(
    RabbitConfig,
    PublisherSpecificationConfig,
):
    routing_address: Address
    message_kwargs: "PublishKwargs"


@dataclass(kw_only=True)
class RabbitPublisherConfig(RabbitConfig, PublisherUsecaseConfig):
    _outer_config: "RabbitBrokerConfig" = field(default_factory=RabbitBrokerConfig)

    routing_address: Address
    message_kwargs: "PublishKwargs"
