from dataclasses import dataclass
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

from typing_extensions import override

from faststream._internal.configs import BrokerConfig, UnderlyingDriverAnnotation
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


def _context_annotations() -> "Mapping[Any, Any]":
    # `annotations` reaches this module through the broker, so the
    # objects a row needs only exist once the package is built.
    from redis.asyncio.client import (
        Pipeline as PipelineDriver,
        Redis as RedisDriver,
    )

    from faststream.redis import annotations
    from faststream.redis.broker.broker import RedisBroker as RedisBrokerDriver
    from faststream.redis.message import (
        RedisBatchStreamMessage as RedisBatchStreamMessageDriver,
        RedisChannelMessage as RedisChannelMessageDriver,
        RedisListMessage as RedisListMessageDriver,
        RedisMessage as RedisMessageDriver,
        RedisStreamMessage as RedisStreamMessageDriver,
    )

    return MappingProxyType(
        {
            RedisDriver: UnderlyingDriverAnnotation(
                annotations.Redis, "faststream.redis.annotations", "Redis"
            ),
            PipelineDriver: UnderlyingDriverAnnotation(
                annotations.Pipeline, "faststream.redis.annotations", "Pipeline"
            ),
            RedisBrokerDriver: UnderlyingDriverAnnotation(
                annotations.RedisBroker, "faststream.redis.annotations", "RedisBroker"
            ),
            RedisMessageDriver: UnderlyingDriverAnnotation(
                annotations.RedisMessage, "faststream.redis.annotations", "RedisMessage"
            ),
            RedisChannelMessageDriver: UnderlyingDriverAnnotation(
                annotations.RedisChannelMessage,
                "faststream.redis.annotations",
                "RedisChannelMessage",
            ),
            RedisStreamMessageDriver: UnderlyingDriverAnnotation(
                annotations.RedisStreamMessage,
                "faststream.redis.annotations",
                "RedisStreamMessage",
            ),
            RedisBatchStreamMessageDriver: UnderlyingDriverAnnotation(
                annotations.RedisBatchStreamMessage,
                "faststream.redis.annotations",
                "RedisBatchStreamMessage",
            ),
            RedisListMessageDriver: UnderlyingDriverAnnotation(
                annotations.RedisListMessage,
                "faststream.redis.annotations",
                "RedisListMessage",
            ),
        },
    )


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer | RedisClusterFastProducer"
    connection: "ConnectionState[Redis[bytes]] | ConnectionState[RedisCluster[bytes]]"

    message_format: type["MessageFormat"]

    @override
    def _default_driver_annotations(self) -> "Mapping[Any, Any]":
        return _context_annotations()

    async def connect(self) -> None:
        self.producer.connect(
            self.fd_config._serializer, codec=self.broker_codec or DefaultCodec()
        )
        await self.connection.connect()

    async def disconnect(self) -> None:
        await self.connection.disconnect()


@dataclass(kw_only=True)
class RedisRouterConfig(BrokerConfig):
    @override
    def _default_driver_annotations(self) -> "Mapping[Any, Any]":
        return _context_annotations()

    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
