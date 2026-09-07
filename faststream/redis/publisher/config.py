from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from faststream.api.configs import (
    PublisherSpecificationConfig,
    PublisherUsecaseConfig,
)
from faststream.redis.configs import RedisBrokerConfig

if TYPE_CHECKING:
    from faststream.redis.parser import MessageFormat


class RedisPublisherSpecificationConfig(PublisherSpecificationConfig):
    pass


@dataclass(kw_only=True)
class RedisPublisherConfig(PublisherUsecaseConfig):
    outer_config: RedisBrokerConfig

    reply_to: str
    headers: dict[str, Any] | None

    _message_format: type["MessageFormat"] | None = None

    @property
    def message_format(self) -> type["MessageFormat"]:
        return self._message_format or self.outer_config.message_format
