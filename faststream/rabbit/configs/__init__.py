from dataclasses import dataclass

from .specification import (
    RabbitPublisherSpecificationConfig,
    RabbitSubscriberSpecificationConfig,
)
from .usecase import RabbitPublisherConfig, RabbitSubscriberConfig


@dataclass(kw_only=True)
class RabbitSubscriberConfigFacade(RabbitSubscriberSpecificationConfig, RabbitSubscriberConfig):
    pass


@dataclass(kw_only=True)
class RabbitPublisherConfigFacade(RabbitPublisherSpecificationConfig, RabbitPublisherConfig):
    pass


__all__ = (
    "RabbitPublisherConfig",
    "RabbitPublisherConfigFacade",
    "RabbitPublisherConfigFacade",
    "RabbitPublisherSpecificationConfig",
    "RabbitSubscriberConfig",
    "RabbitSubscriberConfigFacade",
    "RabbitSubscriberSpecificationConfig",
)
