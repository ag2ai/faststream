from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any

from faststream._internal.configs import BrokerConfig
from faststream._internal.parser import DefaultCodec
from faststream.exceptions import IncorrectState

if TYPE_CHECKING:
    from collections.abc import Mapping

    from redis.asyncio.client import Redis
    from redis.asyncio.cluster import RedisCluster

    from faststream.redis.parser import MessageFormat
    from faststream.redis.publisher.producer import (
        RedisClusterFastProducer,
        RedisFastProducer,
    )

    from .state import ConnectionState


def _context_annotations() -> "Mapping[type[Any], Any]":
    # `annotations` reaches this module through the broker, so it can only be
    # imported once the package is built.
    from faststream.redis.annotations import CONTEXT_ANNOTATIONS

    return CONTEXT_ANNOTATIONS


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer | RedisClusterFastProducer"
    connection: "ConnectionState[Redis[bytes]] | ConnectionState[RedisCluster[bytes]]"

    message_format: type["MessageFormat"]

    underlying_driver_annotations: "Mapping[type[Any], Any]" = field(
        default_factory=_context_annotations
    )

    async def connect(self) -> None:
        self.producer.connect(
            self.fd_config._serializer, codec=self.broker_codec or DefaultCodec()
        )
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class RedisRouterConfig(BrokerConfig):
    underlying_driver_annotations: "Mapping[type[Any], Any]" = field(
        default_factory=_context_annotations
    )

    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
