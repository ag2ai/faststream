from dataclasses import dataclass
from typing import Any

from faststream._internal.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.redis.configs import RedisBrokerConfig


class RedisPublisherSpecificationConfig(PublisherSpecificationConfig):
    pass


@dataclass(kw_only=True)
class RedisPublisherConfig(PublisherUsecaseConfig):
    _outer_config: RedisBrokerConfig

    reply_to: str
    headers: dict[str, Any] | None
