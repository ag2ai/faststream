from collections.abc import AsyncGenerator
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
    RedisBatchStreamMessage as Rbsm,
    RedisChannelMessage as Rcm,
    RedisListMessage as Rlm,
    RedisMessage as Rm,
    RedisStreamMessage as Rsm,
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
    "RedisBatchStreamMessage",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisStreamMessage",
)

RedisMessage = Annotated[Rm, Context("message")]
RedisChannelMessage = Annotated[Rcm, Context("message")]
RedisStreamMessage = Annotated[Rsm, Context("message")]
RedisBatchStreamMessage = Annotated[Rbsm, Context("message")]
RedisListMessage = Annotated[Rlm, Context("message")]

RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]


async def get_pipe(redis: Redis) -> AsyncGenerator[RedisPipeline, None]:
    async with redis.pipeline() as pipe:
        yield pipe


Pipeline = Annotated[RedisPipeline, Depends(get_pipe, cast=False)]
