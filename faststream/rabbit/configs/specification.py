from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.endpoint.publisher import PublisherSpecificationConfig
from faststream._internal.endpoint.subscriber import SubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue

    from .broker import RabbitBrokerConfig


@dataclass(kw_only=True)
class RabbitSpecificationConfig:
    config: "RabbitBrokerConfig"

    queue: "RabbitQueue"
    exchange: "RabbitExchange"


@dataclass(kw_only=True)
class RabbitPublisherSpecificationConfig(
    RabbitSpecificationConfig, PublisherSpecificationConfig
):
    pass


@dataclass(kw_only=True)
class RabbitSubscriberSpecificationConfig(
    RabbitSpecificationConfig, SubscriberSpecificationConfig
):
    pass
