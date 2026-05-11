from collections.abc import Iterable, Mapping, Sequence
from typing import Annotated, Any

from fast_depends import Provider
from fast_depends.dependencies import Dependant
from fast_depends.library.serializer import SerializerProto
from redis.asyncio.connection import BaseParser, Connection, Encoder
from typing_extensions import TypedDict

from faststream._internal.basic_types import LoggerProto
from faststream._internal.context.repository import ContextRepo
from faststream._internal.parser import CodecProto
from faststream._internal.types import BrokerMiddleware, CustomCallable
from faststream.middlewares import AckPolicy
from faststream.redis.broker.registrator import RedisRegistrator
from faststream.redis.parser import MessageFormat
from faststream.security import BaseSecurity
from faststream.specification.schema.extra import Tag, TagDict


class RedisConnectionParams(TypedDict, total=False):
    """Connection-level parameters."""

    host: Annotated[
        str, "Redis host. Extracted from URL if not set. Defaults to ``EMPTY``."
    ]
    port: Annotated[
        str | int, "Redis port. Extracted from URL if not set. Defaults to ``EMPTY``."
    ]
    db: Annotated[str | int, "Redis database number. Defaults to ``EMPTY``."]
    connection_class: Annotated[
        type[Connection], "Connection class. Defaults to ``EMPTY``."
    ]
    client_name: Annotated[str | None, "Redis client name. Defaults to ``None``."]
    health_check_interval: Annotated[float, "Health check interval. Defaults to ``0``."]
    max_connections: Annotated[
        int | None, "Max connections in pool. Defaults to ``None``."
    ]
    socket_timeout: Annotated[
        float | None, "Socket operation timeout. Defaults to ``None``."
    ]
    socket_connect_timeout: Annotated[
        float | None, "Socket connection timeout. Defaults to ``None``."
    ]
    socket_read_size: Annotated[int, "Socket read buffer size. Defaults to ``65536``."]
    socket_keepalive: Annotated[bool, "Enable TCP keepalive. Defaults to ``False``."]
    socket_keepalive_options: Annotated[
        Mapping[int, int | bytes] | None, "TCP keepalive options. Defaults to ``None``."
    ]
    socket_type: Annotated[int, "Socket type. Defaults to ``0``."]
    retry_on_timeout: Annotated[bool, "Retry on timeout. Defaults to ``False``."]
    encoding: Annotated[str, 'Encoding for data. Defaults to ``"utf-8"``.']
    encoding_errors: Annotated[str, 'Encoding error handling. Defaults to ``"strict"``.']
    parser_class: Annotated[
        type[BaseParser], "Parser class. Defaults to ``DefaultParser``."
    ]
    encoder_class: Annotated[type[Encoder], "Encoder class. Defaults to ``Encoder``."]


class RedisBrokerParams(RedisConnectionParams, total=False):
    graceful_timeout: Annotated[
        float | None, "Graceful shutdown timeout. Defaults to ``15.0``."
    ]
    ack_policy: Annotated[
        AckPolicy, "Default acknowledgement policy. Defaults to ``EMPTY``."
    ]
    decoder: Annotated[
        CustomCallable | None, "Custom message decoder. Defaults to ``None``."
    ]
    codec: Annotated[CodecProto | None, "Custom codec. Defaults to ``None``."]
    parser: Annotated[
        CustomCallable | None, "Custom message parser. Defaults to ``None``."
    ]
    dependencies: Annotated[
        Iterable[Dependant], "Subscriber dependencies. Defaults to ``()``."
    ]
    middlewares: Annotated[
        Sequence[BrokerMiddleware[Any, Any]], "Global middlewares. Defaults to ``()``."
    ]
    routers: Annotated[
        Iterable[RedisRegistrator], "Routers to include. Defaults to ``()``."
    ]
    message_format: Annotated[
        type[MessageFormat],
        "Message serialization format. Defaults to ``BinaryMessageFormatV1``.",
    ]
    security: Annotated[BaseSecurity | None, "Security options. Defaults to ``None``."]
    specification_url: Annotated[
        str | None, "AsyncAPI server address. Defaults to ``None``."
    ]
    protocol: Annotated[str | None, "AsyncAPI protocol. Defaults to ``None``."]
    protocol_version: Annotated[
        str | None, 'AsyncAPI protocol version. Defaults to ``"custom"``.'
    ]
    description: Annotated[str | None, "AsyncAPI description. Defaults to ``None``."]
    tags: Annotated[Iterable[Tag | TagDict], "AsyncAPI tags. Defaults to ``()``."]
    logger: Annotated[LoggerProto, "Custom logger. Defaults to ``EMPTY``."]
    log_level: Annotated[int, "Service log level. Defaults to ``logging.INFO``."]
    apply_types: Annotated[bool, "Use FastDepends type casting. Defaults to ``True``."]
    serializer: Annotated[SerializerProto, "Custom serializer. Defaults to ``EMPTY``."]
    provider: Annotated[Provider | None, "FastDepends provider. Defaults to ``None``."]
    context: Annotated[ContextRepo | None, "Context repository. Defaults to ``None``."]


class RedisClusterParams(RedisBrokerParams):
    startup_nodes: Annotated[
        list[tuple[str, int]] | None,
        "Explicit seed node addresses. Auto-discovered when omitted. Defaults to ``None``.",
    ]


CLUSTER_INCOMPATIBLE_PARAMS = frozenset({
    "db",
    "socket_read_size",
    "socket_type",
    "retry_on_timeout",
    "parser_class",
    "encoder_class",
    "connection_class",
    "host",
    "port",
})

NON_CONNECTION_PARAMS = frozenset({
    "graceful_timeout",
    "ack_policy",
    "decoder",
    "codec",
    "parser",
    "dependencies",
    "middlewares",
    "routers",
    "message_format",
    "specification_url",
    "protocol",
    "protocol_version",
    "description",
    "tags",
    "logger",
    "log_level",
    "apply_types",
    "serializer",
    "provider",
    "context",
})
