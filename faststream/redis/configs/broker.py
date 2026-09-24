from dataclasses import dataclass, field
from types import MappingProxyType
from typing import TYPE_CHECKING, Any

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


def _context_annotations_factory() -> "Mapping[Any, Any]":
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
                type_hint=annotations.Redis,
                module="faststream.redis.annotations",
                name="Redis",
            ),
            PipelineDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.Pipeline,
                module="faststream.redis.annotations",
                name="Pipeline",
            ),
            RedisBrokerDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisBroker,
                module="faststream.redis.annotations",
                name="RedisBroker",
            ),
            RedisMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisMessage,
                module="faststream.redis.annotations",
                name="RedisMessage",
            ),
            RedisChannelMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisChannelMessage,
                module="faststream.redis.annotations",
                name="RedisChannelMessage",
            ),
            RedisStreamMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisStreamMessage,
                module="faststream.redis.annotations",
                name="RedisStreamMessage",
            ),
            RedisBatchStreamMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisBatchStreamMessage,
                module="faststream.redis.annotations",
                name="RedisBatchStreamMessage",
            ),
            RedisListMessageDriver: UnderlyingDriverAnnotation(
                type_hint=annotations.RedisListMessage,
                module="faststream.redis.annotations",
                name="RedisListMessage",
            ),
        },
    )


@dataclass(kw_only=True)
class RedisBrokerConfig(BrokerConfig):
    producer: "RedisFastProducer | RedisClusterFastProducer"
    connection: "ConnectionState[Redis[bytes]] | ConnectionState[RedisCluster[bytes]]"

    message_format: type["MessageFormat"]

    default_driver_annotations: "Mapping[Any, Any]" = field(
        default_factory=_context_annotations_factory,
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
    default_driver_annotations: "Mapping[Any, Any]" = field(
        default_factory=_context_annotations_factory,
    )

    @property
    def connection(self) -> ConnectionError:
        raise IncorrectState
