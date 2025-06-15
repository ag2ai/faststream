from typing import TYPE_CHECKING, AsyncIterator

from redis.asyncio.client import Pipeline as _RedisPipeline
from redis.asyncio.client import Redis as _RedisClient
from typing_extensions import Annotated

from faststream import Depends
from faststream.annotations import ContextRepo, Logger, NoCast
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.message import RedisStreamMessage as RSM
from faststream.redis.message import UnifyRedisMessage
from faststream.utils.context import Context

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
)

RedisMessage = Annotated[UnifyRedisMessage, Context("message")]
RedisStreamMessage = Annotated[RSM, Context("message")]
RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]


async def get_pipe(redis: Redis) -> AsyncIterator[RedisPipeline]:
    async with redis.pipeline() as pipe:
        yield pipe


Pipeline = Annotated[RedisPipeline, Depends(get_pipe, cast=False)]
