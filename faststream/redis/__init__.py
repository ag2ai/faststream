from typing import TYPE_CHECKING

from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from faststream._internal.parser import ParserProto

    RedisParserType = ParserProto["Mapping[str, Any]"]  # type: ignore[name-defined]

try:
    from .annotations import (
        Pipeline,
        Redis,
        RedisChannelMessage,
        RedisListMessage,
        RedisMessage,
        RedisStreamMessage,
    )
    from .broker import RedisBroker, RedisPublisher, RedisRoute, RedisRouter
    from .parser import BinaryMessageFormatV1
    from .response import RedisPublishCommand, RedisResponse
    from .schemas import ListSub, PubSub, StreamSub
    from .testing import TestRedisBroker

except ImportError as e:
    if "'redis'" not in e.msg:
        raise

    from faststream.exceptions import INSTALL_FASTSTREAM_REDIS

    raise ImportError(INSTALL_FASTSTREAM_REDIS) from e

__all__ = (
    "BinaryMessageFormatV1",
    "ListSub",
    "Pipeline",
    "PubSub",
    "Redis",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisListMessage",
    "RedisMessage",
    "RedisParserType",
    "RedisPublishCommand",
    "RedisPublisher",
    "RedisResponse",
    "RedisRoute",
    "RedisRouter",
    "RedisStreamMessage",
    "StreamSub",
    "TestApp",
    "TestRedisBroker",
)
