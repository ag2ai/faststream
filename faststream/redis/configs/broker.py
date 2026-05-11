from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
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


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer | RedisClusterFastProducer"
    connection: "ConnectionState[Redis[bytes]] | ConnectionState[RedisCluster[bytes]]"

    message_format: type["MessageFormat"]

    async def connect(self) -> None:
        self.producer.connect(self.fd_config._serializer)
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class RedisRouterConfig(BrokerConfig):
    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
