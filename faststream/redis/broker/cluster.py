import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    Union,
)
from urllib.parse import urlparse

from fast_depends import Provider, dependency_provider
from typing_extensions import override

from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.middlewares import AckPolicy
from faststream.redis.broker.broker import RedisBroker
from faststream.redis.broker.logging import make_redis_logger_state
from faststream.redis.broker.registrator import RedisRegistrator
from faststream.redis.configs import ClusterConnectionState, RedisBrokerConfig
from faststream.redis.configs.state import _CLUSTER_UNSUPPORTED_KEYS
from faststream.redis.parser import BinaryMessageFormatV1, MessageFormat
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.security import parse_security
from faststream.specification.schema import BrokerSpec

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import LoggerProto
    from faststream._internal.parser import CodecProto
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class RedisClusterBroker(RedisBroker):
    """Redis Cluster broker.

    Accepts the same subscription types as :class:`RedisBroker` (channels,
    streams, lists) but connects via ``redis.asyncio.cluster.RedisCluster``
    instead of ``redis.asyncio.client.Redis``.

    .. note::

        Channel (PubSub) subscribers require ``redis-py >= 8.0.0`` which
        added async cluster pubsub support.  Streams and lists work with
        ``redis-py >= 5.0.0``.
    """

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        # Cluster-relevant connection kwargs
        host: str = EMPTY,
        port: str | int = EMPTY,
        client_name: str | None = None,
        health_check_interval: float = 0,
        max_connections: int | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        # Broker args
        graceful_timeout: float | None = 15.0,
        ack_policy: AckPolicy = EMPTY,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[RedisRegistrator] = (),
        message_format: type["MessageFormat"] = BinaryMessageFormatV1,
        security: Optional["BaseSecurity"] = None,
        specification_url: str | None = None,
        protocol: str | None = None,
        protocol_version: str | None = "custom",
        description: str | None = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        provider: Optional["Provider"] = None,
        context: Optional["ContextRepo"] = None,
    ) -> None:
        """Initialize the RedisClusterBroker.

        Args:
            url:
                A Redis Cluster node URL. Defaults to "redis://localhost:6379".
            host:
                The Redis host to connect to. If not provided, it will be extracted from the URL.
            port:
                The Redis port to connect to. If not provided, it will be extracted from the URL.
            client_name:
                The name of the Redis client. Defaults to None.
            health_check_interval:
                The interval at which to perform health checks on the broker. Defaults to 0.
            max_connections:
                The maximum number of connections per node. Defaults to None (uses RedisCluster default of 2^31).
            socket_timeout:
                The timeout for socket operations. Defaults to None.
            socket_connect_timeout:
                The timeout for connecting sockets. Defaults to None.
            socket_keepalive:
                Whether to enable keep-alive on sockets. Defaults to False.
            socket_keepalive_options:
                Options for keep-alive on sockets. Defaults to None.
            encoding:
                The encoding used for sending and receiving data. Defaults to "utf-8".
            encoding_errors:
                How to handle encoding errors. Defaults to "strict".
            graceful_timeout:
                Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down. Defaults to 15.0.
            ack_policy:
                Default acknowledgement policy for all subscribers. Individual subscribers can override.
            decoder:
                Custom decoder object. Defaults to None.
            codec:
                Custom codec object. Defaults to None.
            parser:
                Custom parser object. Defaults to None.
            dependencies:
                Dependencies to apply to all broker subscribers. Defaults to ().
            middlewares:
                Middlewares to apply to all broker publishers/subscribers. Defaults to ().
            routers:
                Routers to apply to broker. Defaults to ().
            message_format:
                What format to use when parsing messages. Defaults to BinaryMessageFormatV1.
            security:
                Security options to connect broker and generate AsyncAPI server security information. Defaults to None.
            specification_url:
                AsyncAPI hardcoded server addresses. Use ``servers`` if not specified. Defaults to None.
            protocol:
                AsyncAPI server protocol. Defaults to None.
            protocol_version:
                AsyncAPI server protocol version. Defaults to "custom".
            description:
                AsyncAPI server description. Defaults to None.
            tags:
                AsyncAPI server tags. Defaults to ().
            logger:
                User specified logger to pass into Context and log service messages. Defaults to EMPTY.
            log_level:
                Service messages log level. Defaults to logging.INFO.
            apply_types:
                Whether to use FastDepends or not. Defaults to True.
            serializer:
                Serializer object. Defaults to EMPTY.
            provider:
                Provider for FastDepends library. Defaults to None.
            context:
                Context repository for FastDepends library. Defaults to None.
        """
        self.message_format = message_format

        if specification_url is None:
            specification_url = url

        if protocol is None:
            url_kwargs = urlparse(specification_url)
            protocol = url_kwargs.scheme

        connection_options = _resolve_cluster_url_options(
            url,
            security=security,
            host=host,
            port=port,
            client_name=client_name,
            health_check_interval=health_check_interval,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            encoding=encoding,
            encoding_errors=encoding_errors,
        )

        connection_state = ClusterConnectionState(connection_options)

        # Skip RedisBroker.__init__ - call its parent directly so we can
        # inject ClusterConnectionState instead of ConnectionState.
        super(RedisBroker, self).__init__(
            **connection_options,
            routers=routers,
            config=RedisBrokerConfig(
                connection=connection_state,
                producer=RedisFastProducer(
                    connection=connection_state,
                    parser=parser,
                    decoder=decoder,
                    message_format=self.message_format,
                    serializer=serializer,
                ),
                message_format=self.message_format,
                # both args
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                broker_codec=codec,
                logger=make_redis_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    provider=provider or dependency_provider,
                    context=context or ContextRepo(),
                ),
                # subscriber args
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                ack_policy=ack_policy,
                extra_context={
                    "broker": self,
                },
            ),
            specification=BrokerSpec(
                description=description,
                url=[specification_url],
                protocol=protocol,
                protocol_version=protocol_version,
                security=security,
                tags=tags,
            ),
        )

    @override
    async def _connect(self) -> Any:
        await self.config.connect()
        return self.config.broker_config.connection.client


def _resolve_cluster_url_options(
    url: str,
    *,
    security: Optional["BaseSecurity"],
    **kwargs: Any,
) -> dict[str, Any]:
    from redis.asyncio.connection import parse_url

    url_options: dict[str, Any] = dict(parse_url(url))
    # parse_url may include keys that RedisCluster doesn't accept (e.g. db).
    # ClusterConnectionState filters these, but we also strip them here to
    # keep the options dict tidy when it is passed as **kwargs to
    # super().__init__().
    for key in _CLUSTER_UNSUPPORTED_KEYS:
        url_options.pop(key, None)

    return {
        **url_options,
        **parse_security(security),
        **{k: v for k, v in kwargs.items() if v is not EMPTY},
    }
