from collections.abc import AsyncIterator
from typing import TYPE_CHECKING, Annotated

from redis.asyncio.client import (
    Pipeline as _RedisPipeline,
    Redis as _RedisClient,
)

from faststream import Depends
from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.message import (
    RedisStreamMessage as RSM,
    UnifyRedisMessage,
)

if TYPE_CHECKING:
    RedisClient = _RedisClient[bytes]
    RedisPipeline = _RedisPipeline[bytes]
else:
    RedisClient = _RedisClient
    RedisPipeline = _RedisPipeline


__all__ = (
    "ContextRepo",
    "Logger",
    "NoCast",
    "Pipeline",
    "Redis",
    "RedisBroker",
    "RedisMessage",
    "RedisStreamMessage",
)

RedisMessage = Annotated[UnifyRedisMessage, Context("message")]
RedisStreamMessage = Annotated[RSM, Context("message")]
RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]


async def get_pipe(redis: Redis) -> AsyncIterator[RedisPipeline]:
    async with redis.pipeline() as pipe:
        yield pipe


Pipeline = Annotated[RedisPipeline, Depends(get_pipe, cast=False)]
