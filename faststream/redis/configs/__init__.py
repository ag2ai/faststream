from dataclasses import dataclass

from .broker import RedisBrokerConfig
from .specification import (
    RedisPublisherSpecificationConfig,
    RedisSubscriberSpecificationConfig,
)
from .state import ConnectionState
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
    "ConnectionState",
    "RedisBrokerConfig",
    "RedisPublisherConfig",
    "RedisPublisherConfigFacade",
    "RedisPublisherSpecificationConfig",
    "RedisSubscriberConfig",
    "RedisSubscriberConfigFacade",
    "RedisSubscriberSpecificationConfig",
)
