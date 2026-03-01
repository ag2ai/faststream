from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.configs import BrokerConfig
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream.redis.parser import MessageFormat
    from faststream.redis.publisher.producer import RedisFastProducer

    from .state import ConnectionState


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer"
    connection: "ConnectionState"

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
