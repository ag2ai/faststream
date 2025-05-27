import logging
from collections.abc import Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
    Union,
    cast,
)
from urllib.parse import urlparse

import anyio
from aio_pika import IncomingMessage, RobustConnection, connect_robust
from typing_extensions import override

from faststream.__about__ import SERVICE_NAME
from faststream._internal.broker.broker import ABCBroker, BrokerUsecase
from faststream._internal.constants import EMPTY
from faststream._internal.di import FastDependsConfig
from faststream.message import gen_cor_id
from faststream.rabbit.configs import RabbitBrokerConfig
from faststream.rabbit.helpers.channel_manager import ChannelManagerImpl
from faststream.rabbit.helpers.declarer import RabbitDeclarerImpl
from faststream.rabbit.publisher.producer import (
    AioPikaFastProducer,
    AioPikaFastProducerImpl,
)
from faststream.rabbit.response import RabbitPublishCommand
from faststream.rabbit.schemas import (
    RABBIT_REPLY,
    Channel,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.security import parse_security
from faststream.rabbit.utils import build_url
from faststream.response.publish_type import PublishType

from .logging import make_rabbit_logger_state
from .registrator import RabbitRegistrator

if TYPE_CHECKING:
    from types import TracebackType

    import aiormq
    from aio_pika import (
        RobustChannel,
        RobustExchange,
        RobustQueue,
    )
    from aio_pika.abc import DateType, HeadersType, SSLOptions, TimeoutType
    from fast_depends.dependencies import Dependant
    from fast_depends.library.serializer import SerializerProto
    from yarl import URL

    from faststream._internal.basic_types import Decorator, LoggerProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.types import AioPikaSendableMessage
    from faststream.rabbit.utils import RabbitClientProperties
    from faststream.security import BaseSecurity
    from faststream.specification.schema.extra import Tag, TagDict


class RabbitBroker(
    RabbitRegistrator,
    BrokerUsecase[IncomingMessage, RobustConnection],
):
    """A class to represent a RabbitMQ broker."""

    url: str

    _producer: "AioPikaFastProducer"
    _channel: Optional["RobustChannel"]

    config: "RabbitBrokerConfig"

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
        default_channel: Optional["Channel"] = None,
        app_id: Optional[str] = SERVICE_NAME,
        # broker base args
        graceful_timeout: Optional[float] = None,
        decoder: Optional["CustomCallable"] = None,
        parser: Optional["CustomCallable"] = None,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[IncomingMessage]"] = (),
        routers: Sequence["ABCBroker[IncomingMessage]"] = (),
        # AsyncAPI args
        security: Optional["BaseSecurity"] = None,
        specification_url: Optional[str] = None,
        protocol: Optional[str] = None,
        protocol_version: Optional[str] = "0.9.1",
        description: Optional[str] = None,
        tags: Iterable[Union["Tag", "TagDict"]] = (),
        # logging args
        logger: Optional["LoggerProto"] = EMPTY,
        log_level: int = logging.INFO,
        # FastDepends args
        apply_types: bool = True,
        serializer: Optional["SerializerProto"] = EMPTY,
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
            app_id: Application name to mark outgoing messages by.
            graceful_timeout: Graceful shutdown timeout. Broker waits for all running subscribers completion before shut down.
            decoder: Custom decoder object.
            parser: Custom parser object.
            dependencies: Dependencies to apply to all broker subscribers.
            middlewares: Middlewares to apply to all broker publishers/subscribers.
            routers: RabbitRouters to build a broker with.
            security: Security options to connect broker and generate AsyncAPI server security information.
            specification_url: AsyncAPI hardcoded server addresses. Use `servers` if not specified.
            protocol: AsyncAPI server protocol.
            protocol_version: AsyncAPI server protocol version.
            description: AsyncAPI server description.
            tags: AsyncAPI server tags.
            logger: User-specified logger to pass into Context and log service messages.
            log_level: Service messages log level.
            apply_types: Whether to use FastDepends or not.
            serializer: FastDepends-compatible serializer to validate incoming messages.
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

        if specification_url is None:
            specification_url = str(amqp_url)

        # respect ascynapi_url argument scheme
        built_asyncapi_url = urlparse(specification_url)
        if protocol is None:
            protocol = built_asyncapi_url.scheme

        cm = ChannelManagerImpl(default_channel)
        declarer = RabbitDeclarerImpl(cm)

        producer = AioPikaFastProducerImpl(
            declarer=declarer,
            decoder=decoder,
            parser=parser,
        )

        super().__init__(
            # connection args
            url=str(amqp_url),
            ssl_context=security_args.get("ssl_context"),
            timeout=timeout,
            fail_fast=fail_fast,
            reconnect_interval=reconnect_interval,
            # Basic args
            routers=routers,
            config=RabbitBrokerConfig(
                channel_manager=cm,
                producer=producer,
                declarer=declarer,
                app_id=app_id,
                virtual_host=built_asyncapi_url.path,
                # both args
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                logger=make_rabbit_logger_state(
                    logger=logger,
                    log_level=log_level,
                ),
                fd_config=FastDependsConfig(
                    use_fastdepends=apply_types,
                    serializer=serializer,
                    get_dependent=_get_dependant,
                    call_decorators=_call_decorators,
                ),
                # subscriber args
                broker_dependencies=dependencies,
                graceful_timeout=graceful_timeout,
                extra_context={
                    "broker": self,
                },
            ),
            # AsyncAPI args
            description=description,
            specification_url=specification_url,
            protocol=protocol or built_asyncapi_url.scheme,
            protocol_version=protocol_version,
            security=security,
            tags=tags,
        )

        self._channel = None

    @override
    async def _connect(self) -> "RobustConnection":
        connection = cast(
            "RobustConnection",
            await connect_robust(**self._connection_kwargs),
        )

        if self._channel is None:
            self.config.connect(connection)
            self._channel = await self.config.channel_manager.get_channel()

        return connection

    async def close(
        self,
        exc_type: Optional[type[BaseException]] = None,
        exc_val: Optional[BaseException] = None,
        exc_tb: Optional["TracebackType"] = None,
    ) -> None:
        await super().close(exc_type, exc_val, exc_tb)

        if self._channel is not None:
            if not self._channel.is_closed:
                await self._channel.close()

            self._channel = None

        if self._connection is not None:
            await self._connection.close()
            self._connection = None

        self.config.disconnect()

    async def start(self) -> None:
        """Connect broker to RabbitMQ and startup all subscribers."""
        await self.connect()
        self._setup()

        await self.declare_queue(RABBIT_REPLY)

        for publisher in self._publishers:
            if publisher.exchange is not None:
                await self.declare_exchange(publisher.exchange)

        await super().start()

    @override
    async def publish(
        self,
        message: "AioPikaSendableMessage" = None,
        queue: Union["RabbitQueue", str] = "",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        # publish options
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        persist: bool = False,
        reply_to: Optional[str] = None,
        correlation_id: Optional[str] = None,
        # message options
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        expiration: Optional["DateType"] = None,
        message_id: Optional[str] = None,
        timestamp: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
        priority: Optional[int] = None,
    ) -> Optional["aiormq.abc.ConfirmationFrameType"]:
        """Publish message directly.

        This method allows you to publish message in not AsyncAPI-documented way. You can use it in another frameworks
        applications or to publish messages from time to time.

        Please, use `@broker.publisher(...)` or `broker.publisher(...).publish(...)` instead in a regular way.

        Args:
            message:
                Message body to send.
            queue:
                Message routing key to publish with.
            exchange:
                Target exchange to publish message to.
            routing_key:
                Message routing key to publish with. Overrides `queue` option if presented.
            mandatory:
                Client waits for confirmation that the message is placed to some queue. RabbitMQ returns message to client if there is no suitable queue.
            immediate:
                Client expects that there is consumer ready to take the message to work. RabbitMQ returns message to client if there is no suitable consumer.
            timeout:
                Send confirmation time from RabbitMQ.
            persist:
                Restore the message on RabbitMQ reboot.
            reply_to:
                Reply message routing key to send with (always sending to default exchange).
            correlation_id:
                Manual message **correlation_id** setter. **correlation_id** is a useful option to trace messages.
            headers:
                Message headers to store metainformation.
            content_type:
                Message **content-type** header. Used by application, not core RabbitMQ. Will be set automatically if not specified.
            content_encoding:
                Message body content encoding, e.g. **gzip**.
            expiration:
                Message expiration (lifetime) in seconds (or datetime or timedelta).
            message_id:
                Arbitrary message id. Generated automatically if not present.
            timestamp:
                Message publish timestamp. Generated automatically if not presented.
            message_type:
                Application-specific message type, e.g. **orders.created**.
            user_id:
                Publisher connection User ID, validated if set.
            priority:
                The message priority (0 by default).

        Returns:
            An optional `aiormq.abc.ConfirmationFrameType` representing the confirmation frame if RabbitMQ is configured to send confirmations.
        """
        cmd = RabbitPublishCommand(
            message,
            routing_key=routing_key or RabbitQueue.validate(queue).routing(),
            exchange=RabbitExchange.validate(exchange),
            correlation_id=correlation_id or gen_cor_id(),
            app_id=self.config.app_id,
            mandatory=mandatory,
            immediate=immediate,
            persist=persist,
            reply_to=reply_to,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_id=message_id,
            message_type=message_type,
            timestamp=timestamp,
            user_id=user_id,
            timeout=timeout,
            priority=priority,
            _publish_type=PublishType.PUBLISH,
        )

        return await super()._basic_publish(cmd, producer=self._producer)

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
        cmd = RabbitPublishCommand(
            message,
            routing_key=routing_key or RabbitQueue.validate(queue).routing(),
            exchange=RabbitExchange.validate(exchange),
            correlation_id=correlation_id or gen_cor_id(),
            app_id=self.config.app_id,
            mandatory=mandatory,
            immediate=immediate,
            persist=persist,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_id=message_id,
            message_type=message_type,
            timestamp=timestamp,
            user_id=user_id,
            timeout=timeout,
            priority=priority,
            _publish_type=PublishType.REQUEST,
        )

        msg: RabbitMessage = await super()._basic_request(cmd, producer=self._producer)
        return msg

    async def declare_queue(self, queue: "RabbitQueue") -> "RobustQueue":
        """Declares queue object in **RabbitMQ**."""
        return await self.config.declarer.declare_queue(queue)

    async def declare_exchange(self, exchange: "RabbitExchange") -> "RobustExchange":
        """Declares exchange object in **RabbitMQ**."""
        return await self.config.declarer.declare_exchange(exchange)

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
