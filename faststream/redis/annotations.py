from collections.abc import AsyncGenerator
from typing import Annotated

from redis.asyncio.client import (
    Pipeline as _RedisPipeline,
    Redis as _RedisClient,
)
from redis.asyncio.cluster import (
    ClusterPipeline as _ClusterPipeline,
    RedisCluster as _RedisClusterClient,
)

from faststream import Depends
from faststream._internal.context import Context
from faststream.annotations import ContextRepo, Logger
from faststream.params import NoCast
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.broker.cluster_broker import RedisClusterBroker as RCB
from faststream.redis.message import (
    RedisBatchStreamMessage as Rbsm,
    RedisChannelMessage as Rcm,
    RedisListMessage as Rlm,
    RedisMessage as Rm,
    RedisStreamMessage as Rsm,
)

RedisClient = _RedisClient
RedisPipeline = _RedisPipeline

__all__ = (
    "ClusterPipeline",
    "ContextRepo",
    "Logger",
    "NoCast",
    "Pipeline",
    "Redis",
    "RedisBatchStreamMessage",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisCluster",
    "RedisClusterBroker",
    "RedisStreamMessage",
)

RedisMessage = Annotated[Rm, Context("message")]
RedisChannelMessage = Annotated[Rcm, Context("message")]
RedisStreamMessage = Annotated[Rsm, Context("message")]
RedisBatchStreamMessage = Annotated[Rbsm, Context("message")]
RedisListMessage = Annotated[Rlm, Context("message")]

RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]

RedisClusterBroker = Annotated[RCB, Context("broker")]
RedisCluster = Annotated[_RedisClusterClient, Context("broker._connection")]


async def get_pipe(redis: Redis) -> AsyncGenerator[RedisPipeline, None]:
    async with redis.pipeline() as pipe:
        yield pipe


Pipeline = Annotated[RedisPipeline, Depends(get_pipe, cast=False)]


async def get_cluster_pipe(redis: Redis) -> AsyncGenerator[RedisPipeline, None]:
    async with redis.pipeline() as pipe:
        yield pipe


ClusterPipeline = Annotated[_ClusterPipeline, Depends(get_cluster_pipe, cast=False)]
