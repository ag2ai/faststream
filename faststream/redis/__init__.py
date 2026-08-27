from typing import TYPE_CHECKING, TypeAlias

from faststream._internal.parser import ParserProto
from faststream._internal.testing.app import TestApp

if TYPE_CHECKING:
    from collections.abc import Mapping
    from typing import Any

RedisParserType: TypeAlias = ParserProto["Mapping[str, Any]"]

try:
    from .annotations import (
        Pipeline,
        Redis,
        RedisBatchStreamMessage,
        RedisChannelMessage,
        RedisListMessage,
        RedisMessage,
        RedisStreamMessage,
    )
    from .broker import (
        RedisBroker,
        RedisClusterBroker,
        RedisPublisher,
        RedisRoute,
        RedisRouter,
        RedisSentinelBroker,
    )
    from .exceptions import StreamClaimUnsupportedError, StreamGroupNotFoundError
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
    "RedisBatchStreamMessage",
    "RedisBroker",
    "RedisChannelMessage",
    "RedisClusterBroker",
    "RedisListMessage",
    "RedisMessage",
    "RedisParserType",
    "RedisPublishCommand",
    "RedisPublisher",
    "RedisResponse",
    "RedisRoute",
    "RedisRouter",
    "RedisSentinelBroker",
    "RedisStreamMessage",
    "StreamClaimUnsupportedError",
    "StreamGroupNotFoundError",
    "StreamSub",
    "TestApp",
    "TestRedisBroker",
)
