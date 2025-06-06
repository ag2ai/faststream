from dataclasses import dataclass

from .broker import NatsBrokerConfig
from .specification import (
    NatsPublisherSpecificationConfig,
    NatsSubscriberSpecificationConfig,
)
from .usecase import NatsPublisherConfig, NatsSubscriberConfig


@dataclass(kw_only=True)
class NatsSubscriberConfigFacade(
    NatsSubscriberSpecificationConfig, NatsSubscriberConfig
):
    pass


@dataclass(kw_only=True)
class NatsPublisherConfigFacade(NatsPublisherSpecificationConfig, NatsPublisherConfig):
    pass


__all__ = (
    "NatsBrokerConfig",
    "NatsPublisherConfig",
    "NatsPublisherConfigFacade",
    "NatsPublisherSpecificationConfig",
    "NatsSubscriberConfig",
    "NatsSubscriberConfigFacade",
    "NatsSubscriberSpecificationConfig",
)
