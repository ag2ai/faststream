from typing import Annotated

from redis.asyncio.client import Redis as RedisClient

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.message import BaseMessage as RM  # noqa: N814

from .cluster import RedisClusterRouter
from .fastapi import RedisRouter

__all__ = (
    "ContextRepo",
    "Logger",
    "Redis",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisClusterRouter",
    "RedisRouter",
)

RedisChannelMessage = Annotated[RM, Context("message")]
RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]
