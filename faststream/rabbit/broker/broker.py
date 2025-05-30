import logging
import warnings
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Iterable,
    Optional,
    Sequence,
    Type,
    Union,
    cast,
)
from urllib.parse import urlparse

import anyio
from aio_pika import connect_robust
from typing_extensions import Annotated, Doc, deprecated, override

from faststream.__about__ import SERVICE_NAME
from faststream.broker.message import gen_cor_id
from faststream.exceptions import NOT_CONNECTED_YET
from faststream.rabbit.broker.logging import RabbitLoggingBroker
from faststream.rabbit.broker.registrator import RabbitRegistrator
from faststream.rabbit.helpers import ChannelManager, RabbitDeclarer
from faststream.rabbit.publisher.producer import AioPikaFastProducer
from faststream.rabbit.schemas import (
    RABBIT_REPLY,
    Channel,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.security import parse_security
from faststream.rabbit.subscriber.asyncapi import AsyncAPISubscriber
from faststream.rabbit.utils import build_url
from faststream.types import EMPTY

if TYPE_CHECKING:
    from ssl import SSLContext
    from types import TracebackType

    from aio_pika import (
        IncomingMessage,
        RobustChannel,
        RobustConnection,
        RobustExchange,
        RobustQueue,
    )
    from aio_pika.abc import DateType, HeadersType, SSLOptions, TimeoutType
    from fast_depends.dependencies import Depends
    from yarl import URL

    from faststream.asyncapi import schema as asyncapi
    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.rabbit.utils import RabbitClientProperties
    from faststream.security import BaseSecurity
    from faststream.types import AnyDict, Decorator, LoggerProto


class RabbitBroker(
    RabbitRegistrator,
    RabbitLoggingBroker,
):
    """A class to represent a RabbitMQ broker."""

    url: str
    _producer: Optional["AioPikaFastProducer"]

    declarer: Optional["RabbitDeclarer"]
    _channel: Optional["RobustChannel"]

    def __init__(
        self,
        url: Union[
            str, "URL", None
        ] = "amqp://guest:guest@localhost:5672/",  # pragma: allowlist secret
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        virtualhost: Optional[str] = None,
        ssl_options: Optional["SSLOptions"] = None,
        client_properties: Optional["RabbitClientProperties"] = None,
        timeout: "TimeoutType" = None,
        fail_fast: bool = True,
        reconnect_interval: "TimeoutType" = 5.0,
        default_channel: Optional[Channel] = None,
        channel_number: Annotated[
            Optional[int],
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(channel_number=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = None,
        publisher_confirms: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(publisher_confirms=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = True,
        on_return_raises: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(on_return_raises=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        max_consumers: Annotated[
            Optional[int],
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(prefetch_count=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = None,
        app_id: Optional[str] = SERVICE_NAME,
        graceful_timeout: Optional[float] = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Depends"] = (),
        middlewares: Sequence["BrokerMiddleware[IncomingMessage]"] = (),
        security: Optional["BaseSecurity"] = None,
        asyncapi_url: Optional[str] = None,
        protocol: Optional[str] = None,
        protocol_version: Optional[str] = "0.9.1",
        description: Optional[str] = None,
        tags: Optional[Iterable[Union["asyncapi.Tag", "asyncapi.TagDict"]]] = None,
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        log_fmt: Annotated[
            Optional[str],
            deprecated(
                "Argument `log_fmt` is deprecated since 0.5.42 and will be removed in 0.6.0. "
                "Pass a pre-configured `logger` instead."
            ),
        ] = EMPTY,
        apply_types: bool = True,
        validate: bool = True,
        _get_dependant: Optional[Callable[..., Any]] = None,
        _call_decorators: Iterable["Decorator"] = (),
    ) -> None:
        """Initialize the RabbitBroker.

        Args:
            url: RabbitMQ destination location to connect.
            host: Destination host. This option overrides `url` option host.
            port: Destination port. This option overrides `url` option port.
            virtualhost: RabbitMQ virtual host to use in the current broker connection.
            ssl_options: Extra ssl options to establish connection.
            client_properties: Add custom client capability.
            timeout: Connection establishment timeout.
            fail_fast: Broker startup raises `AMQPConnectionError` if RabbitMQ is unreachable.
            reconnect_interval: Time to sleep between reconnection attempts.
            default_channel: Default channel settings to use.
            channel_number: Specify the channel number explicitly. Deprecated in **FastStream 0.5.39**.
            publisher_confirms: If `True`, the `publish` method will return `bool` type after publish is complete.
                Otherwise, it will return `None`. Deprecated in **FastStream 0.5.39**.
            on_return_raises: Raise an :class:`aio_pika.exceptions.DeliveryError` when mandatory message will be returned.
                Deprecated in **FastStream 0.5.39**.
            max_consumers: RabbitMQ channel `qos` / `prefetch_count` option. It limits max messages processing
                in the same time count. Deprecated in **FastStream 0.5.39**.
            app_id: Application name to mark outgoing messages by.
            graceful_timeout: Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder: Custom decoder object.
            parser: Custom parser object.
            dependencies: Dependencies to apply to all broker subscribers.
            middlewares: Middlewares to apply to all broker publishers/subscribers.
            security: Security options to connect broker and generate AsyncAPI server security information.
            asyncapi_url: AsyncAPI hardcoded server addresses. Use `servers` if not specified.
            protocol: AsyncAPI server protocol.
            protocol_version: AsyncAPI server protocol version.
            description: AsyncAPI server description.
            tags: AsyncAPI server tags.
            logger: User-specified logger to pass into Context and log service messages.
            log_level: Service messages log level.
            log_fmt: Default logger log format.
            apply_types: Whether to use FastDepends or not.
            validate: Whether to cast types using Pydantic validation.
            _get_dependant: Custom library dependant generator callback.
            _call_decorators: Any custom decorator to apply to wrapped functions.
        """
        security_args = parse_security(security)

        amqp_url = build_url(
            url,
            host=host,
            port=port,
            virtualhost=virtualhost,
            ssl_options=ssl_options,
            client_properties=client_properties,
            login=security_args.get("login"),
            password=security_args.get("password"),
            ssl=security_args.get("ssl"),
        )

        if asyncapi_url is None:
            asyncapi_url = str(amqp_url)

        # respect ascynapi_url argument scheme
        built_asyncapi_url = urlparse(asyncapi_url)
        self.virtual_host = built_asyncapi_url.path
        if protocol is None:
            protocol = built_asyncapi_url.scheme

        channel_settings = default_channel or Channel(
            channel_number=channel_number,
            publisher_confirms=publisher_confirms,
            on_return_raises=on_return_raises,
            prefetch_count=max_consumers,
        )

        super().__init__(
            url=str(amqp_url),
            ssl_context=security_args.get("ssl_context"),
            timeout=timeout,
            fail_fast=fail_fast,
            reconnect_interval=reconnect_interval,
            # channel args
            channel_settings=channel_settings,
            # Basic args
            graceful_timeout=graceful_timeout,
            dependencies=dependencies,
            decoder=decoder,
            parser=parser,
            middlewares=middlewares,
            # AsyncAPI args
            description=description,
            asyncapi_url=asyncapi_url,
            protocol=protocol or built_asyncapi_url.scheme,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
            # Logging args
            logger=logger,
            log_level=log_level,
            log_fmt=log_fmt,
            # FastDepends args
            apply_types=apply_types,
            validate=validate,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

        self.app_id = app_id

        self._channel = None
        self.declarer = None

    @property
    def _subscriber_setup_extra(self) -> "AnyDict":
        return {
            **super()._subscriber_setup_extra,
            "app_id": self.app_id,
            "virtual_host": self.virtual_host,
            "declarer": self.declarer,
        }

    @property
    def _publisher_setup_extra(self) -> "AnyDict":
        return {
            **super()._publisher_setup_extra,
            "app_id": self.app_id,
            "virtual_host": self.virtual_host,
        }

    @override
    async def connect(  # type: ignore[override]
        self,
        url: Union[str, "URL", None] = EMPTY,
        *,
        host: Optional[str] = None,
        port: Optional[int] = None,
        virtualhost: Optional[str] = None,
        ssl_options: Optional["SSLOptions"] = None,
        client_properties: Optional["RabbitClientProperties"] = None,
        security: Optional["BaseSecurity"] = None,
        timeout: "TimeoutType" = None,
        fail_fast: bool = EMPTY,
        reconnect_interval: "TimeoutType" = EMPTY,
        # channel args
        default_channel: Optional[Channel] = None,
        channel_number: Annotated[
            Optional[int],
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(channel_number=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = EMPTY,
        publisher_confirms: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(publisher_confirms=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = EMPTY,
        on_return_raises: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.39**. "
                "Please, use `default_channel=Channel(on_return_raises=...)` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = EMPTY,
    ) -> "RobustConnection":
        """Connect broker object to RabbitMQ.

        To startup subscribers too you should use `broker.start()` after/instead this method.

        Args:
            url: RabbitMQ destination location to connect.
            host: Destination host. This option overrides `url` option host.
            port: Destination port. This option overrides `url` option port.
            virtualhost: RabbitMQ virtual host to use in the current broker connection.
            ssl_options: Extra ssl options to establish connection.
            client_properties: Add custom client capability.
            security: Security options to connect broker and generate AsyncAPI server security information.
            timeout: Connection establishement timeout.
            fail_fast: Broker startup raises `AMQPConnectionError` if RabbitMQ is unreachable.
            reconnect_interval: Time to sleep between reconnection attempts.
            default_channel: Default channel settings to use.
            channel_number: Specify the channel number explicit.
            publisher_confirms: if `True` the `publish` method will
                return `bool` type after publish is complete.
                Otherwise it will returns `None`.
            on_return_raises: raise an :class:`aio_pika.exceptions.DeliveryError`
                when mandatory message will be returned
        """
        kwargs: AnyDict = {}

        if not default_channel and (
            channel_number is not EMPTY
            or publisher_confirms is not EMPTY
            or on_return_raises is not EMPTY
        ):
            default_channel = Channel(
                channel_number=None if channel_number is EMPTY else channel_number,
                publisher_confirms=True
                if publisher_confirms is EMPTY
                else publisher_confirms,
                on_return_raises=False
                if on_return_raises is EMPTY
                else on_return_raises,
            )

        if default_channel:
            kwargs["channel_settings"] = default_channel

        if timeout:
            kwargs["timeout"] = timeout

        if fail_fast is not EMPTY:
            kwargs["fail_fast"] = fail_fast

        if reconnect_interval is not EMPTY:
            kwargs["reconnect_interval"] = reconnect_interval

        url = None if url is EMPTY else url

        if url or any(
            (host, port, virtualhost, ssl_options, client_properties, security)
        ):
            security_args = parse_security(security)

            kwargs["url"] = build_url(
                url,
                host=host,
                port=port,
                virtualhost=virtualhost,
                ssl_options=ssl_options,
                client_properties=client_properties,
                login=security_args.get("login"),
                password=security_args.get("password"),
                ssl=security_args.get("ssl"),
            )

            if ssl_context := security_args.get("ssl_context"):
                kwargs["ssl_context"] = ssl_context

        if kwargs:
            warnings.warn(
                "`RabbitBroker().connect(...) options were "
                "deprecated in **FastStream 0.5.40**. "
                "Please, use `RabbitBroker(...)` instead. "
                "All these options will be removed in **FastStream 0.6.0**.",
                DeprecationWarning,
                stacklevel=2,
            )

        connection = await super().connect(**kwargs)

        return connection

    @override
    async def _connect(  # type: ignore[override]
        self,
        url: str,
        *,
        fail_fast: bool,
        reconnect_interval: "TimeoutType",
        timeout: "TimeoutType",
        ssl_context: Optional["SSLContext"],
        channel_settings: Channel,
    ) -> "RobustConnection":
        connection = cast(
            "RobustConnection",
            await connect_robust(
                url,
                timeout=timeout,
                ssl_context=ssl_context,
                reconnect_interval=reconnect_interval,
                fail_fast=fail_fast,
            ),
        )

        ch_manager = self._channel_manager = ChannelManager(
            connection,
            default_channel=channel_settings,
        )
        declarer = self.declarer = RabbitDeclarer(ch_manager)

        if self._channel is None:  # pragma: no branch
            self._channel = await ch_manager.get_channel(channel_settings)

            await declarer.declare_queue(RABBIT_REPLY)

            self._producer = AioPikaFastProducer(
                declarer=declarer,
                decoder=self._decoder,
                parser=self._parser,
            )

            if qos := channel_settings.prefetch_count:
                c = AsyncAPISubscriber.build_log_context(
                    None,
                    RabbitQueue(""),
                    RabbitExchange(""),
                )
                self._log(f"Set max consumers to {qos}", extra=c)

        return connection

    async def _close(
        self,
        exc_type: Optional[Type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        if self._channel is not None:
            if not self._channel.is_closed:
                await self._channel.close()

            self._channel = None

        self.declarer = None
        self._producer = None

        if self._connection is not None:
            await self._connection.close()

        await super()._close(exc_type, exc_val, exc_tb)

    async def start(self) -> None:
        """Connect broker to RabbitMQ and startup all subscribers."""
        await super().start()

        assert self.declarer, NOT_CONNECTED_YET  # nosec B101

        for publisher in self._publishers.values():
            if publisher.exchange is not None:
                await self.declare_exchange(publisher.exchange)

        for subscriber in self._subscribers.values():
            self._log(
                f"`{subscriber.call_name}` waiting for messages",
                extra=subscriber.get_log_context(None),
            )
            await subscriber.start()

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "AioPikaSendableMessage" = None,
        queue: Union["RabbitQueue", str] = "",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        persist: bool = False,
        reply_to: Optional[str] = None,
        rpc: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.17**. Please, use `request` method instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        rpc_timeout: Annotated[
            Optional[float],
            deprecated(
                "Deprecated in **FastStream 0.5.17**. Please, use `request` method with `timeout` instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = 30.0,
        raise_timeout: Annotated[
            bool,
            deprecated(
                "Deprecated in **FastStream 0.5.17**. `request` always raises TimeoutError instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        # message args
        correlation_id: Optional[str] = None,
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        expiration: Optional["DateType"] = None,
        message_id: Optional[str] = None,
        timestamp: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
        priority: Optional[int] = None,
    ) -> Optional[Any]:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.

        Args:
            message: Message body to send.
            queue: Message routing key to publish with.
            exchange: Target exchange to publish message to.
            routing_key: Message routing key to publish with. Overrides `queue` option if presented.
            mandatory: Client waits for confirmation that the message is placed to some queue.
            RabbitMQ returns message to client if there is no suitable queue.
            immediate: Client expects that there is consumer ready to take the message to work.
            RabbitMQ returns message to client if there is no suitable consumer.
            timeout: Send confirmation time from RabbitMQ.
            persist: Restore the message on RabbitMQ reboot.
            reply_to: Reply message routing key to send with (always sending to default exchange).
            rpc: Whether to wait for reply in blocking mode.
            rpc_timeout: RPC reply waiting time.
            raise_timeout: Whether to raise `TimeoutError` or return `None` at **rpc_timeout**.
            correlation_id: Manual message **correlation_id** setter.
            **correlation_id** is a useful option to trace messages.
            headers: Message headers to store metainformation.
            content_type: Message **content-type** header.
            Used by application, not core RabbitMQ. Will be set automatically if not specified.
            content_encoding: Message body content encoding, e.g. **gzip**.
            expiration: Message expiration (lifetime) in seconds (or datetime or timedelta).
            message_id: Arbitrary message id. Generated automatically if not present.
            timestamp: Message publish timestamp. Generated automatically if not present.
            message_type: Application-specific message type, e.g. **orders.created**.
            user_id: Publisher connection User ID, validated if set.
            priority: The message priority (0 by default).
        """
        routing = routing_key or RabbitQueue.validate(queue).routing
        correlation_id = correlation_id or gen_cor_id()

        return await super().publish(
            message,
            producer=self._producer,
            routing_key=routing,
            app_id=self.app_id,
            exchange=exchange,
            mandatory=mandatory,
            immediate=immediate,
            persist=persist,
            reply_to=reply_to,
            headers=headers,
            correlation_id=correlation_id,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_id=message_id,
            timestamp=timestamp,
            message_type=message_type,
            user_id=user_id,
            timeout=timeout,
            priority=priority,
            rpc=rpc,
            rpc_timeout=rpc_timeout,
            raise_timeout=raise_timeout,
        )

    @override
    async def request(  # type: ignore[override]
        self,
        message: "AioPikaSendableMessage" = None,
        queue: Union["RabbitQueue", str] = "",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        persist: bool = False,
        # message args
        correlation_id: Optional[str] = None,
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        expiration: Optional["DateType"] = None,
        message_id: Optional[str] = None,
        timestamp: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
        priority: Optional[int] = None,
    ) -> "RabbitMessage":
        """Make a synchronous request to RabbitMQ.

        This method uses Direct Reply-To pattern to send a message and wait for a reply.
        It is a blocking call and will wait for a reply until timeout.

        Args:
            message: Message body to send.
            queue: Message routing key to publish with.
            exchange: Target exchange to publish message to.
            routing_key: Message routing key to publish with. Overrides `queue` option if presented.
            mandatory: Client waits for confirmation that the message is placed to some queue.
            RabbitMQ returns message to client if there is no suitable queue.
            immediate: Client expects that there is a consumer ready to take the message to work.
            RabbitMQ returns message to client if there is no suitable consumer.
            timeout: Send confirmation time from RabbitMQ.
            persist: Restore the message on RabbitMQ reboot.
            correlation_id: Manual message **correlation_id** setter. **correlation_id** is a useful option to trace messages.
            headers: Message headers to store metainformation.
            content_type: Message **content-type** header. Used by application, not core RabbitMQ.
            Will be set automatically if not specified.
            content_encoding: Message body content encoding, e.g. **gzip**.
            expiration: Message expiration (lifetime) in seconds (or datetime or timedelta).
            message_id: Arbitrary message id. Generated automatically if not present.
            timestamp: Message publish timestamp. Generated automatically if not present.
            message_type: Application-specific message type, e.g. **orders.created**.
            user_id: Publisher connection User ID, validated if set.
            priority: The message priority (0 by default).
        """
        routing = routing_key or RabbitQueue.validate(queue).routing
        correlation_id = correlation_id or gen_cor_id()

        msg: RabbitMessage = await super().request(
            message,
            producer=self._producer,
            correlation_id=correlation_id,
            routing_key=routing,
            app_id=self.app_id,
            exchange=exchange,
            mandatory=mandatory,
            immediate=immediate,
            persist=persist,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_id=message_id,
            timestamp=timestamp,
            message_type=message_type,
            user_id=user_id,
            timeout=timeout,
            priority=priority,
        )
        return msg

    async def declare_queue(
        self,
        queue: Annotated[
            "RabbitQueue",
            Doc("Queue object to create."),
        ],
    ) -> "RobustQueue":
        """Declares queue object in **RabbitMQ**."""
        assert self.declarer, NOT_CONNECTED_YET  # nosec B101
        return await self.declarer.declare_queue(queue)

    async def declare_exchange(
        self,
        exchange: Annotated[
            "RabbitExchange",
            Doc("Exchange object to create."),
        ],
    ) -> "RobustExchange":
        """Declares exchange object in **RabbitMQ**."""
        assert self.declarer, NOT_CONNECTED_YET  # nosec B101
        return await self.declarer.declare_exchange(exchange)

    @override
    async def ping(self, timeout: Optional[float]) -> bool:
        sleep_time = (timeout or 10) / 10

        with anyio.move_on_after(timeout) as cancel_scope:
            if self._connection is None:
                return False

            while True:
                if cancel_scope.cancel_called:
                    return False

                if self._connection.connected.is_set():
                    return True

                await anyio.sleep(sleep_time)

        return False
