from dataclasses import dataclass

from .broker import RabbitBrokerConfig
from .specification import (
    RabbitPublisherSpecificationConfig,
    RabbitSubscriberSpecificationConfig,
)
from .usecase import RabbitPublisherConfig, RabbitSubscriberConfig


@dataclass(kw_only=True)
class RabbitSubscriberConfigFacade(
    RabbitSubscriberSpecificationConfig,
    RabbitSubscriberConfig,
):
    pass


@dataclass(kw_only=True)
class RabbitPublisherConfigFacade(
    RabbitPublisherSpecificationConfig,
    RabbitPublisherConfig,
):
    pass


__all__ = (
    "RabbitBrokerConfig",
    "RabbitPublisherConfig",
    "RabbitPublisherConfigFacade",
    "RabbitPublisherConfigFacade",
    "RabbitPublisherSpecificationConfig",
    "RabbitSubscriberConfig",
    "RabbitSubscriberConfigFacade",
    "RabbitSubscriberSpecificationConfig",
)
