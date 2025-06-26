from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.rabbit.configs.base import RabbitConfig, RabbitEndpointConfig

if TYPE_CHECKING:
    from .options import PublishKwargs


@dataclass(kw_only=True)
class RabbitPublisherSpecificationConfig(
    RabbitConfig,
    PublisherSpecificationConfig,
):
    routing_key: str
    message_kwargs: "PublishKwargs"


@dataclass(kw_only=True)
class RabbitPublisherConfig(RabbitEndpointConfig, PublisherUsecaseConfig):
    routing_key: str
    message_kwargs: "PublishKwargs"
