from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue

    from .broker import RabbitBrokerConfig


@dataclass(kw_only=True)
class RabbitConfig:
    queue: "RabbitQueue"
    exchange: "RabbitExchange"


@dataclass(kw_only=True)
class RabbitEndpointConfig(RabbitConfig):
    _outer_config: "RabbitBrokerConfig"
