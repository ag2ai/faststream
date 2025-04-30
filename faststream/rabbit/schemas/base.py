from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue


@dataclass
class RabbitBaseConfigs:
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
