from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.publisher.configs import (
    PublisherUseCaseConfigs,
)

if TYPE_CHECKING:
    from faststream.rabbit.publisher.usecase import PublishKwargs
    from faststream.rabbit.schemas.exchange import RabbitExchange
    from faststream.rabbit.schemas.queue import RabbitQueue


@dataclass
class RabbitPublisherBaseConfigs(PublisherUseCaseConfigs):
    routing_key: str
    queue: "RabbitQueue"
    exchange: "RabbitExchange"
    message_kwargs: "PublishKwargs"
