from collections.abc import Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Final

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from redis.asyncio.client import Redis
    from redis.asyncio.cluster import RedisCluster

    from faststream.redis.parser import MessageFormat
    from faststream.redis.publisher.producer import (
        RedisClusterFastProducer,
        RedisFastProducer,
    )

    from .state import ConnectionState


# Driver class to the context annotation that injects it, both as import
# paths so this table needs no imports of its own.
CONTEXT_ANNOTATIONS: Final[Mapping[str, str]] = MappingProxyType(
    {
        "redis.asyncio.client.Redis": "faststream.redis.annotations.Redis",
        "redis.asyncio.client.Pipeline": "faststream.redis.annotations.Pipeline",
        "faststream.redis.broker.broker.RedisBroker": "faststream.redis.annotations.RedisBroker",
        "faststream.redis.message.RedisMessage": "faststream.redis.annotations.RedisMessage",
        "faststream.redis.message.RedisChannelMessage": "faststream.redis.annotations.RedisChannelMessage",
        "faststream.redis.message.RedisStreamMessage": "faststream.redis.annotations.RedisStreamMessage",
        "faststream.redis.message.RedisBatchStreamMessage": "faststream.redis.annotations.RedisBatchStreamMessage",
        "faststream.redis.message.RedisListMessage": "faststream.redis.annotations.RedisListMessage",
    },
)


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer | RedisClusterFastProducer"
    connection: "ConnectionState[Redis[bytes]] | ConnectionState[RedisCluster[bytes]]"

    message_format: type["MessageFormat"]

    underlying_driver_annotations: "Mapping[str, str]" = CONTEXT_ANNOTATIONS

    async def connect(self) -> None:
        self.producer.connect(
            self.fd_config._serializer, codec=self.broker_codec or DefaultCodec()
        )
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class RedisRouterConfig(BrokerConfig):
    underlying_driver_annotations: "Mapping[str, str]" = CONTEXT_ANNOTATIONS

    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
