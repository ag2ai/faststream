from dataclasses import dataclass

from .specification import (
    RedisPublisherSpecificationConfig,
    RedisSubscriberSpecificationConfig,
)
from .usecase import RedisPublisherConfig, RedisSubscriberConfig


@dataclass(kw_only=True)
class RedisSubscriberConfigFacade(
    RedisSubscriberSpecificationConfig,
    RedisSubscriberConfig,
):
    pass


@dataclass(kw_only=True)
class RedisPublisherConfigFacade(
    RedisPublisherSpecificationConfig,
    RedisPublisherConfig,
):
    pass


__all__ = (
    "RedisPublisherConfig",
    "RedisPublisherConfigFacade",
    "RedisPublisherSpecificationConfig",
    "RedisSubscriberConfig",
    "RedisSubscriberConfigFacade",
    "RedisSubscriberSpecificationConfig",
)
