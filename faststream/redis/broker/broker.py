import logging
import warnings
from collections.abc import Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    TypeAlias,
    Union,
)
from urllib.parse import urlparse

import anyio
from anyio import move_on_after
from fast_depends import Provider, dependency_provider
from redis.asyncio.connection import (
    Connection,
    DefaultParser,
    Encoder,
    parse_url,
)
from redis.exceptions import ConnectionError
from typing_extensions import deprecated, overload, override

from faststream._internal.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.context.repository import ContextRepo
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.redis.configs import ConnectionState, RedisBrokerConfig
from faststream.redis.message import UnifyRedisDict
from faststream.redis.parser import (
    BinaryMessageFormatV1,
    JSONMessageFormat,
    MessageFormat,
)
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.redis.security import parse_security
from faststream.response.publish_type import PublishType
from faststream.specification.schema import BrokerSpec

from .logging import make_redis_logger_state
from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from redis.asyncio.client import Pipeline, Redis
    from redis.asyncio.connection import BaseParser
    from typing_extensions import TypedDict

    from faststream._internal.basic_types import LoggerProto, SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.redis.message import RedisChannelMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    class RedisInitKwargs(TypedDict, total=False):
        host: str | None
        port: str | int | None
        db: str | int | None
        client_name: str | None
        health_check_interval: float | None
        max_connections: int | None
        socket_timeout: float | None
        socket_connect_timeout: float | None
        socket_read_size: int | None
        socket_keepalive: bool | None
        socket_keepalive_options: Mapping[int, int | bytes] | None
        socket_type: int | None
        retry_on_timeout: bool | None
        encoding: str | None
        encoding_errors: str | None
        parser_class: type["BaseParser"] | None
        connection_class: type["Connection"] | None
        encoder_class: type["Encoder"] | None


Channel: TypeAlias = str


class RedisBroker(
    RedisRegistrator,
    BrokerUsecase[UnifyRedisDict, "Redis[bytes]"],
):
    """Redis broker."""

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        host: str = EMPTY,
        port: str | int = EMPTY,
        db: str | int = EMPTY,
        connection_class: type["Connection"] = EMPTY,
        client_name: str | None = None,
        health_check_interval: float = 0,
        max_connections: int | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = None,
        socket_read_size: int = 65536,
        socket_keepalive: bool = False,
        socket_keepalive_options: Mapping[int, int | bytes] | None = None,
        socket_type: int = 0,
        retry_on_timeout: bool = False,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        parser_class: type["BaseParser"] = DefaultParser,
        encoder_class: type["Encoder"] = Encoder,
        graceful_timeout: float | None = 15.0,
        decoder: Optional["CustomCallable"] = None,
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
        """Initialized the RedisBroker.

        Args:
            url:
                The Redis connection URL. Defaults to "redis://localhost:6379".
            host:
                The Redis host to connect to. If not provided, it will be extracted from the URL.
            port:
                The Redis port to connect to. If not provided, it will be extracted from the URL.
            db:
                The Redis database to use. If not provided, it will be extracted from the URL.
            connection_class:
                The class to use for establishing connections. Defaults to EMPTY.
            client_name:
                The name of the Redis client. Defaults to None.
            health_check_interval:
                The interval at which to perform health checks on the broker. Defaults to 0.
            max_connections:
                The maximum number of connections to establish. Defaults to None.
            socket_timeout:
                The timeout for socket operations. Defaults to None.
            socket_connect_timeout:
                The timeout for connecting sockets. Defaults to None.
            socket_read_size:
                The size of the buffer used for reading from sockets. Defaults to 65536.
            socket_keepalive:
                Whether to enable keep-alive on sockets. Defaults to False.
            socket_keepalive_options:
                Options for keep-alive on sockets. Defaults to None.
            socket_type:
                The type of socket to use (if supported by your platform). Defaults to 0.
            retry_on_timeout:
                Whether to retry operations that timeout. Defaults to False.
            encoding:
                The encoding used for sending and receiving data. Defaults to "utf-8".
            encoding_errors:
                How to handle encoding errors. Defaults to "strict".
            parser_class:
                The class to use for parsing messages. Defaults to DefaultParser.
            encoder_class:
                The class to use for encoding messages. Defaults to Encoder.
            graceful_timeout:
                Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down. Defaults to 15.0.
            decoder:
                Custom decoder object. Defaults to None.
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
                AsyncAPI hardcoded server addresses. Use `servers` if not specified. Defaults to None.
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
        if message_format == JSONMessageFormat:
            warnings.warn(
                "JSONMessageFormat has been deprecated and will be removed in version 0.7. "
                "Instead, use BinaryMessageFormatV1 when initializing broker",
                category=DeprecationWarning,
                stacklevel=2,
            )

        self.message_format = message_format

        if specification_url is None:
            specification_url = url

        if protocol is None:
            url_kwargs = urlparse(specification_url)
            protocol = url_kwargs.scheme

        connection_options = _resolve_url_options(
            url,
            security=security,
            host=host,
            port=port,
            db=db,
            client_name=client_name,
            health_check_interval=health_check_interval,
            max_connections=max_connections,
            socket_timeout=socket_timeout,
            socket_connect_timeout=socket_connect_timeout,
            socket_read_size=socket_read_size,
            socket_keepalive=socket_keepalive,
            socket_keepalive_options=socket_keepalive_options,
            socket_type=socket_type,
            retry_on_timeout=retry_on_timeout,
            encoding=encoding,
            encoding_errors=encoding_errors,
            parser_class=parser_class,
            connection_class=connection_class,
            encoder_class=encoder_class,
        )

        connection_state = ConnectionState(connection_options)

        super().__init__(
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
    async def _connect(self) -> "Redis[bytes]":
        await self.config.connect()
        return self.config.broker_config.connection.client

    async def stop(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().stop(exc_type, exc_val, exc_tb)
        await self.config.disconnect()
        self._connection = None

    @deprecated(
        "Deprecated in **FastStream 0.5.44**. "
        "Please, use `stop` method instead. "
        "Method `close` will be removed in **FastStream 0.7.0**.",
        category=DeprecationWarning,
        stacklevel=1,
    )
    async def close(
        self,
        exc_type: type[BaseException] | None = None,
        exc_val: BaseException | None = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await self.stop(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        await self.connect()
        await super().start()

    @overload
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: None = None,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str = ...,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> bytes: ...

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: str | None = None,
        *,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        correlation_id: str | None = None,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int | bytes:
        """Publish message directly.

        This method allows you to publish a message in a non-AsyncAPI-documented way.
        It can be used in other frameworks or to publish messages at specific intervals.

        Args:
            message:
                Message body to send.
            channel:
                Redis PubSub object name to send message.
            reply_to:
                Reply message destination PubSub object name.
            headers:
                Message headers to store metainformation.
            correlation_id:
                Manual message correlation_id setter. correlation_id is a useful option to trace messages.
            list:
                Redis List object name to send message.
            stream:
                Redis Stream object name to send message.
            maxlen:
                Redis Stream maxlen publish option. Remove eldest message if maxlen exceeded.
            pipeline:
                Redis pipeline to use for publishing messages.

        Returns:
            int: The result of the publish operation, typically the number of messages published.
        """
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            reply_to=reply_to,
            headers=headers,
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )

        result: int | bytes = await super()._basic_publish(
            cmd,
            producer=self.config.producer,
        )
        return result

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        channel: str | None = None,
        *,
        list: str | None = None,
        stream: str | None = None,
        maxlen: int | None = None,
        correlation_id: str | None = None,
        headers: dict[str, Any] | None = None,
        timeout: float | None = 30.0,
    ) -> "RedisChannelMessage":
        cmd = RedisPublishCommand(
            message,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            headers=headers,
            timeout=timeout,
            _publish_type=PublishType.REQUEST,
            message_format=self.message_format,
        )
        msg: RedisChannelMessage = await super()._basic_request(
            cmd,
            producer=self.config.producer,
        )
        return msg

    @override
    async def publish_batch(  # type: ignore[override]
        self,
        *messages: "SendableMessage",
        list: str,
        correlation_id: str | None = None,
        reply_to: str = "",
        headers: dict[str, Any] | None = None,
        pipeline: Optional["Pipeline[bytes]"] = None,
    ) -> int:
        """Publish multiple messages to Redis List by one request.

        Args:
            *messages: Messages bodies to send.
            list: Redis List object name to send messages.
            correlation_id: Manual message **correlation_id** setter. **correlation_id** is a useful option to trace messages.
            reply_to: Reply message destination PubSub object name.
            headers: Message headers to store metainformation.
            pipeline: Redis pipeline to use for publishing messages.

        Returns:
            int: The result of the batch publish operation.
        """
        cmd = RedisPublishCommand(
            *messages,
            list=list,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            pipeline=pipeline,
            _publish_type=PublishType.PUBLISH,
            message_format=self.message_format,
        )

        result: int = await self._basic_publish_batch(
            cmd,
            producer=self.config.producer,
        )
        return result

    @override
    async def ping(self, timeout: float | None = 3) -> bool:
        sleep_time = (timeout or 10) / 10

        with move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                try:
                    if await self._connection.ping():
                        return True

                except ConnectionError:
                    pass

                await anyio.sleep(sleep_time)

        return False


def _resolve_url_options(
    url: str,
    *,
    security: Optional["BaseSecurity"],
    **kwargs: Any,
) -> dict[str, Any]:
    return {
        **dict(parse_url(url)),
        **parse_security(security),
        **{k: v for k, v in kwargs.items() if v is not EMPTY},
    }
