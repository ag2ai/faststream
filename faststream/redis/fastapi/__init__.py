import warnings
from typing import Annotated

from redis.asyncio.client import Redis as RedisClient

from faststream._internal.fastapi.context import Context, ContextRepo, Logger
from faststream.redis.broker.broker import RedisBroker as RB
from faststream.redis.message import BaseMessage as RM  # noqa: N814

from .fastapi import RedisRouter, RedisSentinelRouter

warnings.warn(
    "The integration has been moved to the faststream_fastapi package"
    " and will be removed in 1.0.0 version."
    "\n`pip install faststream_fastapi`"
    "\nhttps://github.com/faststream-community/faststream_fastapi",
    DeprecationWarning,
    stacklevel=2,
)

__all__ = (
    "Context",
    "ContextRepo",
    "Logger",
    "Redis",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisRouter",
    "RedisSentinelRouter",
)

RedisChannelMessage = Annotated[RM, Context("message")]
RedisBroker = Annotated[RB, Context("broker")]
Redis = Annotated[RedisClient, Context("broker._connection")]
