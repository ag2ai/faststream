import logging
import warnings
from functools import partial
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Mapping,
    Optional,
    Sequence,
    Type,
    Union,
)
from urllib.parse import urlparse

import anyio
from anyio import move_on_after
from redis.asyncio.client import Pipeline, Redis
from redis.asyncio.connection import (
    Connection,
    ConnectionPool,
    DefaultParser,
    Encoder,
    parse_url,
)
from redis.exceptions import ConnectionError
from typing_extensions import Annotated, Doc, TypeAlias, deprecated, override

from faststream.__about__ import __version__
from faststream.broker.message import gen_cor_id
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.redis.broker.logging import RedisLoggingBroker
from faststream.redis.broker.registrator import RedisRegistrator
from faststream.redis.publisher.producer import RedisFastProducer
from faststream.redis.security import parse_security
from faststream.types import EMPTY

if TYPE_CHECKING:
    from types import TracebackType

    from fast_depends.dependencies import Depends
    from redis.asyncio.connection import BaseParser
    from typing_extensions import TypedDict, Unpack

    from faststream.asyncapi import schema as asyncapi
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.redis.message import BaseMessage, RedisMessage
    from faststream.security import BaseSecurity
    from faststream.types import (
        AnyDict,
        AsyncFunc,
        DecodedMessage,
        Decorator,
        LoggerProto,
        SendableMessage,
    )

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
        parser_class: Optional[Type["BaseParser"]]
        connection_class: Optional[Type["Connection"]]
        encoder_class: Optional[Type["Encoder"]]


Channel: TypeAlias = str


class RedisBroker(
    RedisRegistrator,
    RedisLoggingBroker,
):
    """Redis broker."""

    url: str
    _producer: Optional[RedisFastProducer]

    def __init__(
        self,
        url: str = "redis://localhost:6379",
        *,
        host: str = EMPTY,
        port: Union[str, int] = EMPTY,
        db: Union[str, int] = EMPTY,
        connection_class: Type["Connection"] = EMPTY,
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
        parser_class: Type["BaseParser"] = DefaultParser,
        encoder_class: Type["Encoder"] = Encoder,
        # broker args
        graceful_timeout: Annotated[
            Optional[float],
            Doc(
                "Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down."
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
            Iterable["Depends"],
            Doc("Dependencies to apply to all broker subscribers."),
        ] = (),
        middlewares: Annotated[
            Sequence["BrokerMiddleware[BaseMessage]"],
            Doc("Middlewares to apply to all broker publishers/subscribers."),
        ] = (),
        # AsyncAPI args
        security: Annotated[
            Optional["BaseSecurity"],
            Doc(
                "Security options to connect broker and generate AsyncAPI server security information."
            ),
        ] = None,
        asyncapi_url: Annotated[
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
            Optional[Iterable[Union["asyncapi.Tag", "asyncapi.TagDict"]]],
            Doc("AsyncAPI server tags."),
        ] = None,
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
            deprecated(
                "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                "Pass a pre-configured `logger` instead."
            ),
            Doc("Default logger log format."),
        ] = EMPTY,
        # FastDepends args
        apply_types: Annotated[
            bool,
            Doc("Whether to use FastDepends or not."),
        ] = True,
        validate: Annotated[
            bool,
            Doc("Whether to cast types using Pydantic validation."),
        ] = True,
        _get_dependant: Annotated[
            Optional[Callable[..., Any]],
            Doc("Custom library dependant generator callback."),
        ] = None,
        _call_decorators: Annotated[
            Iterable["Decorator"],
            Doc("Any custom decorator to apply to wrapped functions."),
        ] = (),
    ) -> None:
        self._producer = None

        if asyncapi_url is None:
            asyncapi_url = url

        if protocol is None:
            url_kwargs = urlparse(asyncapi_url)
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
            # AsyncAPI
            description=description,
            asyncapi_url=asyncapi_url,
            protocol=protocol,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
            # logging
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            # FastDepends args
            apply_types=apply_types,
            validate=validate,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

    @override
    async def connect(  # type: ignore[override]
        self,
        url: Optional[str] = EMPTY,
        **kwargs: "Unpack[RedisInitKwargs]",
    ) -> "Redis[bytes]":
        """Connect to the Redis server."""
        if url is not EMPTY or kwargs:
            warnings.warn(
                "`RedisBroker().connect(...) options were "
                "deprecated in **FastStream 0.5.40**. "
                "Please, use `RedisBroker(...)` instead. "
                "All these options will be removed in **FastStream 0.6.0**.",
                DeprecationWarning,
                stacklevel=2,
            )

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
        connection_class: Type["Connection"],
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
        parser_class: Type["BaseParser"],
        encoder_class: Type["Encoder"],
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
        self._producer = RedisFastProducer(
            connection=client,
            parser=self._parser,
            decoder=self._decoder,
        )
        return client

    async def _close(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        if self._connection is not None:
            await self._connection.aclose()  # type: ignore[attr-defined]

        await super()._close(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        await super().start()

        for handler in self._subscribers.values():
            self._log(
                f"`{handler.call_name}` waiting for messages",
                extra=handler.get_log_context(None),
            )
            await handler.start()

    @property
    def _subscriber_setup_extra(self) -> "AnyDict":
        return {
            **super()._subscriber_setup_extra,
            "connection": self._connection,
        }

    @override
    async def publish(  # type: ignore[override]
        self,
        message: Annotated[
            "SendableMessage",
            Doc("Message body to send."),
        ] = None,
        channel: Annotated[
            Optional[str],
            Doc("Redis PubSub object name to send message."),
        ] = None,
        *,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        headers: Annotated[
            Optional["AnyDict"],
            Doc("Message headers to store metainformation."),
        ] = None,
        correlation_id: Annotated[
            Optional[str],
            Doc(
                "Manual message **correlation_id** setter. "
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        list: Annotated[
            Optional[str],
            Doc("Redis List object name to send message."),
        ] = None,
        stream: Annotated[
            Optional[str],
            Doc("Redis Stream object name to send message."),
        ] = None,
        maxlen: Annotated[
            Optional[int],
            Doc(
                "Redis Stream maxlen publish option. "
                "Remove eldest message if maxlen exceeded."
            ),
        ] = None,
        rpc: Annotated[
            bool,
            Doc("Whether to wait for reply in blocking mode."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            Doc("RPC reply waiting time."),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            Doc(
                "Whetever to raise `TimeoutError` or return `None` at **rpc_timeout**. "
                "RPC request returns `None` at timeout by default."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.17**. "
                "`request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
    ) -> Optional["DecodedMessage"]:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.
        """
        return await super().publish(
            message,
            producer=self._producer,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            reply_to=reply_to,
            headers=headers,
            rpc=rpc,
            rpc_timeout=rpc_timeout,
            raise_timeout=raise_timeout,
            pipeline=pipeline,
        )

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
        msg: RedisMessage = await super().request(
            message,
            producer=self._producer,
            correlation_id=correlation_id or gen_cor_id(),
            channel=channel,
            list=list,
            stream=stream,
            maxlen=maxlen,
            headers=headers,
            timeout=timeout,
        )
        return msg

    async def publish_batch(
        self,
        *msgs: Annotated[
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
                "**correlation_id** is a useful option to trace messages."
            ),
        ] = None,
        pipeline: Annotated[
            Optional["Pipeline[bytes]"],
            Doc(
                "Optional Redis `Pipeline` object to batch multiple commands. "
                "Use it to group Redis operations for optimized execution and reduced latency."
            ),
        ] = None,
    ) -> None:
        """Publish multiple messages to Redis List by one request."""
        assert self._producer, NOT_CONNECTED_YET  # nosec B101

        correlation_id = correlation_id or gen_cor_id()

        call: AsyncFunc = self._producer.publish_batch

        for m in self._middlewares[::-1]:
            call = partial(m(None).publish_scope, call)

        await call(
            *msgs,
            list=list,
            correlation_id=correlation_id,
            pipeline=pipeline,
        )

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
