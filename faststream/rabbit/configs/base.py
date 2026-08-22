from dataclasses import dataclass
from typing import TYPE_CHECKING, Union

if TYPE_CHECKING:
    from faststream.rabbit.schemas import RabbitExchange, RabbitQueue


@dataclass(kw_only=True)
class RabbitConfig:
    """How an endpoint's RabbitMQ addresses were declared, before they are read.

    A queue or an exchange arrives here as the user wrote it — a name or a
    prepared object — and is turned into a value object by the read layer in
    `faststream.rabbit.address`, not here.
    """

    queue: Union["RabbitQueue", str]
    exchange: Union["RabbitExchange", str]
