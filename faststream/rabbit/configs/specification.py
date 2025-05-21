from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.endpoint.publisher import PublisherSpecificationConfig
from faststream._internal.endpoint.subscriber import SubscriberSpecificationConfig

if TYPE_CHECKING:
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue


@dataclass(kw_only=True)
class RabbitSpecificationConfig:
    queue: "RabbitQueue"
    exchange: "RabbitExchange"


@dataclass(kw_only=True)
class RabbitPublisherSpecificationConfig(RabbitSpecificationConfig, PublisherSpecificationConfig):
    pass


@dataclass(kw_only=True)
class RabbitSubscriberSpecificationConfig(RabbitSpecificationConfig, SubscriberSpecificationConfig):
    pass
