from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


@dataclass(kw_only=True)
class RabbitConfig:
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
