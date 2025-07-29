from typing import Annotated

from redis.asyncio.client import Redis as RedisClient

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.message import BaseMessage as RM  # noqa: N814

from .fastapi import RedisRouter

__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "Redis",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisRouter",
)

RedisChannelMessage = Annotated[RM, Context("message")]
RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]
