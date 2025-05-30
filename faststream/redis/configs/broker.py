from dataclasses import dataclass
from typing import TYPE_CHECKING

from faststream._internal.broker import BrokerConfig
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from faststream.redis.publisher.producer import RedisFastProducer

    from .state import ConnectionState


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer"
    connection: "ConnectionState"

    def __or__(self, value: "BrokerConfig", /) -> "RedisBrokerConfig":
        return RedisBrokerConfig(
            connection=self.connection,
            **self._merge_configs(value),
        )

    async def connect(self) -> None:
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class RedisRouterConfig(BrokerConfig):
    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
