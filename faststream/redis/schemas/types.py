from collections.abc import Iterable, Mapping, Sequence
from ssl import VerifyMode
from typing import Any

from fast_depends import Provider
from fast_depends.dependencies import Dependant
from fast_depends.library.serializer import SerializerProto
from redis.asyncio.connection import BaseParser, Connection, Encoder
from redis.asyncio.retry import Retry
from typing_extensions import Required, TypedDict

from faststream._internal.basic_types import LoggerProto
from faststream._internal.context.repository import ContextRepo
from faststream._internal.parser import CodecProto
from faststream._internal.types import BrokerMiddleware, CustomCallable, IdGenerator
from faststream.middlewares import AckPolicy
from faststream.redis.broker.registrator import RedisRegistrator
from faststream.redis.parser import MessageFormat
from faststream.security import BaseSecurity
from faststream.specification.schema.extra import Tag, TagDict


class RedisConnectionParams(TypedDict, total=False):
    """Connection-level parameters."""

    host: str
    """Redis host. Extracted from URL if not set. Defaults to ``EMPTY``."""

    port: str | int
    """Redis port. Extracted from URL if not set. Defaults to ``EMPTY``."""

    db: str | int
    """Redis database number. Defaults to ``EMPTY``."""

    connection_class: type[Connection]
    """Connection class. Defaults to ``EMPTY``."""

    client_name: str | None
    """Redis client name. Defaults to ``None``."""

    health_check_interval: float
    """Health check interval. Defaults to ``0``."""

    max_connections: int | None
    """Max connections in pool. Defaults to ``None``."""

    socket_timeout: float | None
    """Socket operation timeout. Defaults to ``None``."""

    socket_connect_timeout: float | None
    """Socket connection timeout. Defaults to ``None``."""

    socket_read_size: int
    """Socket read buffer size. Defaults to ``65536``."""

    socket_keepalive: bool
    """Enable TCP keepalive. Defaults to ``False``."""

    socket_keepalive_options: Mapping[int, int | bytes] | None
    """TCP keepalive options. Defaults to ``None``."""

    socket_type: int
    """Socket type. Defaults to ``0``."""

    retry: Retry | None
    """Retry object. Defaults to ``None``."""

    retry_on_error: list[Any]
    """List of exceptions for which you need to retry. Defaults to ``[]``."""

    retry_on_timeout: bool
    """Retry on timeout. Defaults to ``False``."""

    encoding: str
    """Encoding for data. Defaults to ``"utf-8"``."""

    encoding_errors: str
    """Encoding error handling. Defaults to ``"strict"``."""

    parser_class: type[BaseParser]
    """Parser class. Defaults to ``DefaultParser``."""

    encoder_class: type[Encoder]
    """Encoder class. Defaults to ``Encoder``."""


class RedisBrokerParams(RedisConnectionParams, total=False):
    graceful_timeout: float | None
    """Graceful shutdown timeout. Defaults to ``15.0``."""

    ack_policy: AckPolicy
    """Default acknowledgement policy. Defaults to ``EMPTY``."""

    id_generator: IdGenerator
    """Factory used to generate `correlation_id` when a publish/request call doesn't set one.

    Defaults to `gen_cor_id` (uuid4-based).
    """

    decoder: CustomCallable | None
    """Custom message decoder. Defaults to ``None``."""

    codec: CodecProto | None
    """Custom codec. Defaults to ``None``."""

    parser: CustomCallable | None
    """Custom message parser. Defaults to ``None``."""

    dependencies: Iterable[Dependant]
    """Subscriber dependencies. Defaults to ``()``."""

    middlewares: Sequence[BrokerMiddleware[Any, Any]]
    """Global middlewares. Defaults to ``()``."""

    routers: Iterable[RedisRegistrator]
    """Routers to include. Defaults to ``()``."""

    message_format: type[MessageFormat]
    """Message serialization format. Defaults to ``BinaryMessageFormatV1``."""

    security: BaseSecurity | None
    """Security options. Defaults to ``None``."""

    specification_url: str | None
    """AsyncAPI server address. Defaults to ``None``."""

    protocol: str | None
    """AsyncAPI protocol. Defaults to ``None``."""

    protocol_version: str | None
    """AsyncAPI protocol version. Defaults to ``"custom"``."""

    description: str | None
    """AsyncAPI description. Defaults to ``None``."""

    tags: Iterable[Tag | TagDict]
    """AsyncAPI tags. Defaults to ``()``."""

    logger: LoggerProto
    """Custom logger. Defaults to ``EMPTY``."""

    log_level: int
    """Service log level. Defaults to ``logging.INFO``."""

    apply_types: bool
    """Use FastDepends type casting. Defaults to ``True``."""

    serializer: SerializerProto
    """Custom serializer. Defaults to ``EMPTY``."""

    provider: Provider | None
    """FastDepends provider. Defaults to ``None``."""

    context: ContextRepo | None
    """Context repository. Defaults to ``None``."""


class RedisClusterParams(RedisBrokerParams, total=False):
    startup_nodes: Iterable[tuple[str, int]]
    """Explicit seed node addresses. Auto-discovered when omitted.

    Defaults to ``None``.
    """

    # RedisCluster takes no `ssl_context`: TLS is tuned through these options instead.
    ssl: bool
    """Connect over TLS. Defaults to ``False``."""

    ssl_ca_certs: str | None
    """Path to the CA certificates file. Defaults to ``None``."""

    ssl_ca_data: str | None
    """CA certificates as a PEM string. Defaults to ``None``."""

    ssl_cert_reqs: str | VerifyMode
    """Whether the server certificate is required. Defaults to ``"required"``."""

    ssl_certfile: str | None
    """Path to the client certificate. Defaults to ``None``."""

    ssl_keyfile: str | None
    """Path to the client private key. Defaults to ``None``."""

    ssl_check_hostname: bool
    """Verify the server hostname. Default follows redis-py."""


class RedisSentinelParams(RedisBrokerParams, total=False):
    sentinels: Required[Sequence[tuple[str, int]]]
    """Redis Sentinel ``(host, port)`` nodes to discover the master from. Required."""

    sentinel_master_name: Required[str]
    """Sentinel master group name. Required."""

    sentinel_kwargs: Mapping[str, Any] | None
    """Connection kwargs for the Sentinel nodes themselves. Defaults to ``None``."""


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


SENTINEL_PARAMS = frozenset({
    "sentinels",
    "sentinel_master_name",
    "sentinel_kwargs",
})

NON_CONNECTION_PARAMS = frozenset({
    "graceful_timeout",
    "ack_policy",
    "id_generator",
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
