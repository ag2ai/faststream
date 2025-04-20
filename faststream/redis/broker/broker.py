import logging
from collections.abc import Iterable, Mapping, Sequence
from typing import (
    TYPE_CHECKING,
    Annotated,
    Any,
    Callable,
    Optional,
    Union,
)
from urllib.parse import urlparse

import anyio
from anyio import move_on_after
from redis.asyncio.client import Redis
from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    DefaultParser,
    Encoder,
    parse_url,
)
from redis.exceptions import ConnectionError
from typing_extensions import Doc, TypeAlias, deprecated, overload, override

from faststream.__about__ import __version__
from faststream._internal.broker.broker import BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream.message import gen_cor_id
from faststream.redis.message import UnifyRedisDict
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.response import RedisPublishCommand
from faststream.redis.security import parse_security
from faststream.response.publish_type import PublishType

from .logging import make_redis_logger_state
from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from redis.asyncio.connection import BaseParser
    from typing_extensions import TypedDict, Unpack

    from faststream._internal.basic_types import (
        AnyDict,
        Decorator,
        LoggerProto,
        SendableMessage,
    )
    from faststream._internal.broker.abc_broker import ABCBroker
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.redis.message import BaseMessage, RedisMessage
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict

    class RedisInitKwargs(TypedDict, total=False):
        host: Optional[str]
        port: Union[str, int, None]
        db: Union[str, int, None]
        client_name: Optional[str]
        health_check_interval: Optional[float]
        max_connections: Optional[int]
        socket_timeout: Optional[float]
        socket_connect_timeout: Optional[float]
        socket_read_size: Optional[int]
        socket_keepalive: Optional[bool]
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]]
        socket_type: Optional[int]
        retry_on_timeout: Optional[bool]
        encoding: Optional[str]
        encoding_errors: Optional[str]
        decode_responses: Optional[bool]
        parser_class: Optional[type["BaseParser"]]
        connection_class: Optional[type["Connection"]]
        encoder_class: Optional[type["Encoder"]]


Channel: TypeAlias = str


class RedisBroker(
    RedisRegistrator,
    BrokerUsecase[UnifyRedisDict, "Redis[bytes]"],
):
    """Redis broker."""

    url: str
    _producer: "RedisFastProducer"

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        host: str = EMPTY,
        port: Union[str, int] = EMPTY,
        db: Union[str, int] = EMPTY,
        connection_class: type["Connection"] = EMPTY,
        client_name: Optional[str] = None,
        health_check_interval: float = 0,
        max_connections: Optional[int] = None,
        socket_timeout: Optional[float] = None,
        socket_connect_timeout: Optional[float] = None,
        socket_read_size: int = 65536,
        socket_keepalive: bool = False,
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]] = None,
        socket_type: int = 0,
        retry_on_timeout: bool = False,
        encoding: str = "utf-8",
        encoding_errors: str = "strict",
        decode_responses: bool = False,
        parser_class: type["BaseParser"] = DefaultParser,
        encoder_class: type["Encoder"] = Encoder,
        # broker args
        graceful_timeout: Annotated[
            Optional[float],
            Doc(
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.",
            ),
        ] = 15.0,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Custom decoder object."),
        ] = None,
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Custom parser object."),
        ] = None,
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc("Dependencies to apply to all broker subscribers."),
        ] = (),
        middlewares: Annotated[
            Sequence["BrokerMiddleware[BaseMessage]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        routers: Annotated[
            Sequence["ABCBroker[BaseMessage]"],
            Doc("Routers to apply to broker."),
        ] = (),
        # AsyncAPI args
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security information.",
            ),
        ] = None,
        specification_url: Annotated[
            Optional[str],
            Doc("AsyncAPI hardcoded server addresses. Use `servers` if not specified."),
        ] = None,
        protocol: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol."),
        ] = None,
        protocol_version: Annotated[
            Optional[str],
            Doc("AsyncAPI server protocol version."),
        ] = "custom",
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI server description."),
        ] = None,
        tags: Annotated[
            Iterable[Union["Tag", "TagDict"]],
            Doc("AsyncAPI server tags."),
        ] = (),
        # logging args
        logger: Annotated[
            Optional["LoggerProto"],
            Doc("User specified logger to pass into Context and log service messages."),
        ] = EMPTY,
        log_level: Annotated[
            int,
            Doc("Service messages log level."),
        ] = logging.INFO,
        log_fmt: Annotated[
            Optional[str],
            deprecated("Use `logger` instead. Will be removed in the 0.7.0 release."),
            Doc("Default logger log format."),
        ] = None,
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ] = True,
        serializer: Optional["SerializerProto"] = EMPTY,
        _get_dependant: Annotated[
            Optional[Callable[..., Any]],
            Doc("Custom library dependant generator callback."),
        ] = None,
        _call_decorators: Annotated[
            Iterable["Decorator"],
            Doc("Any custom decorator to apply to wrapped functions."),
        ] = (),
    ) -> None:
        if specification_url is None:
            specification_url = url

        if protocol is None:
            url_kwargs = urlparse(specification_url)
            protocol = url_kwargs.scheme

        super().__init__(
            url=url,
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
            decode_responses=decode_responses,
            parser_class=parser_class,
            connection_class=connection_class,
            encoder_class=encoder_class,
            # Basic args
            # broker base
            graceful_timeout=graceful_timeout,
            dependencies=dependencies,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            routers=routers,
            # AsyncAPI
            description=description,
            specification_url=specification_url,
            protocol=protocol,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
            # logging
            logger_state=make_redis_logger_state(
                logger=logger,
                log_level=log_level,
                log_fmt=log_fmt,
            ),
            # FastDepends args
            apply_types=apply_types,
            serializer=serializer,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

        self._state.patch_value(
            producer=RedisFastProducer(
                parser=self._parser,
                decoder=self._decoder,
            )
        )

    @override
    async def connect(  # type: ignore[override]
        self,
        url: Optional[str] = EMPTY,
        **kwargs: "Unpack[RedisInitKwargs]",
    ) -> "Redis[bytes]":
        """Connect to the Redis server."""
        if url is not EMPTY:
            connect_kwargs: AnyDict = {
                "url": url,
                **kwargs,
            }
        else:
            connect_kwargs = dict(kwargs).copy()

        return await super().connect(**connect_kwargs)

    @override
    async def _connect(  # type: ignore[override]
        self,
        url: str,
        *,
        host: str,
        port: Union[str, int],
        db: Union[str, int],
        connection_class: type["Connection"],
        client_name: Optional[str],
        health_check_interval: float,
        max_connections: Optional[int],
        socket_timeout: Optional[float],
        socket_connect_timeout: Optional[float],
        socket_read_size: int,
        socket_keepalive: bool,
        socket_keepalive_options: Optional[Mapping[int, Union[int, bytes]]],
        socket_type: int,
        retry_on_timeout: bool,
        encoding: str,
        encoding_errors: str,
        decode_responses: bool,
        parser_class: type["BaseParser"],
        encoder_class: type["Encoder"],
    ) -> "Redis[bytes]":
        url_options: AnyDict = {
            **dict(parse_url(url)),
            **parse_security(self.security),
            "client_name": client_name,
            "health_check_interval": health_check_interval,
            "max_connections": max_connections,
            "socket_timeout": socket_timeout,
            "socket_connect_timeout": socket_connect_timeout,
            "socket_read_size": socket_read_size,
            "socket_keepalive": socket_keepalive,
            "socket_keepalive_options": socket_keepalive_options,
            "socket_type": socket_type,
            "retry_on_timeout": retry_on_timeout,
            "encoding": encoding,
            "encoding_errors": encoding_errors,
            "decode_responses": decode_responses,
            "parser_class": parser_class,
            "encoder_class": encoder_class,
        }

        if port is not EMPTY:
            url_options["port"] = port
        if host is not EMPTY:
            url_options["host"] = host
        if db is not EMPTY:
            url_options["db"] = db
        if connection_class is not EMPTY:
            url_options["connection_class"] = connection_class

        pool = ConnectionPool(
            **url_options,
            lib_name="faststream",
            lib_version=__version__,
        )

        client: Redis[bytes] = Redis.from_pool(pool)  # type: ignore[attr-defined]
        self._producer.connect(client)
        return client

    async def close(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().close(exc_type, exc_val, exc_tb)

        self._producer.disconnect()

        if self._connection is not None:
            await self._connection.aclose()  # type: ignore[attr-defined]
            self._connection = None

    async def start(self) -> None:
        await self.connect()
        self._setup()
        await super().start()

    @property
    def _subscriber_setup_extra(self) -> "AnyDict":
        return {
            **super()._subscriber_setup_extra,
            "connection": self._connection,
        }

    @overload
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: Optional[str] = None,
        *,
        reply_to: str = "",
        headers: Optional["AnyDict"] = None,
        correlation_id: Optional[str] = None,
        list: Optional[str] = None,
        stream: None = None,
        maxlen: Optional[int] = None,
    ) -> int: ...

    @overload
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: Optional[str] = None,
        *,
        reply_to: str = "",
        headers: Optional["AnyDict"] = None,
        correlation_id: Optional[str] = None,
        list: Optional[str] = None,
        stream: str,
        maxlen: Optional[int] = None,
    ) -> bytes: ...

    @override
    async def publish(
        self,
        message: "SendableMessage" = None,
        channel: Optional[str] = None,
        *,
        reply_to: str = "",
        headers: Optional["AnyDict"] = None,
        correlation_id: Optional[str] = None,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
    ) -> Union[int, bytes]:
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
            _publish_type=PublishType.PUBLISH,
        )
        return await super()._basic_publish(cmd, producer=self._producer)

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        channel: Optional[str] = None,
        *,
        list: Optional[str] = None,
        stream: Optional[str] = None,
        maxlen: Optional[int] = None,
        correlation_id: Optional[str] = None,
        headers: Optional["AnyDict"] = None,
        timeout: Optional[float] = 30.0,
    ) -> "RedisMessage":
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
        )
        msg: RedisMessage = await super()._basic_request(cmd, producer=self._producer)
        return msg

    async def publish_batch(
        self,
        *messages: Annotated[
            "SendableMessage",
            Doc("Messages bodies to send."),
        ],
        list: Annotated[
            str,
            Doc("Redis List object name to send messages."),
        ],
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages.",
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
    ) -> int:
        """Publish multiple messages to Redis List by one request."""
        cmd = RedisPublishCommand(
            *messages,
            list=list,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            _publish_type=PublishType.PUBLISH,
        )

        return await self._basic_publish_batch(cmd, producer=self._producer)

    @override
    async def ping(self, timeout: Optional[float]) -> bool:
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
