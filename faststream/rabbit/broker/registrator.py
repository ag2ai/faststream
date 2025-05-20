from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Sequence, Union, cast

from typing_extensions import Annotated, deprecated, override

from faststream.broker.core.abc import ABCBroker
from faststream.broker.utils import default_filter
from faststream.exceptions import SetupError
from faststream.rabbit.publisher.asyncapi import AsyncAPIPublisher
from faststream.rabbit.publisher.usecase import PublishKwargs
from faststream.rabbit.schemas import (
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.subscriber.asyncapi import AsyncAPISubscriber
from faststream.rabbit.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from aio_pika import IncomingMessage
    from aio_pika.abc import DateType, HeadersType, TimeoutType
    from fast_depends.dependencies import Depends

    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
        Filter,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.rabbit.message import RabbitMessage
    from faststream.rabbit.schemas import Channel, ReplyConfig
    from faststream.types import AnyDict


class RabbitRegistrator(ABCBroker["IncomingMessage"]):
    """Includable to RabbitBroker router."""

    _subscribers: Dict[int, "AsyncAPISubscriber"]
    _publishers: Dict[int, "AsyncAPIPublisher"]

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: Union[str, "RabbitQueue"],
        exchange: Union[str, "RabbitExchange", None] = None,
        *,
        consume_args: Optional["AnyDict"] = None,
        dependencies: Iterable["Depends"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[RabbitMessage]"] = (),
        channel: Optional["Channel"] = None,
        reply_config: Annotated[
            Optional["ReplyConfig"],
            deprecated(
                "Deprecated in **FastStream 0.5.16**. "
                "Please, use `RabbitResponse` object as a handler return instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = None,
        filter: Annotated[
            "Filter[RabbitMessage]",
            deprecated(
                "Deprecated in **FastStream 0.5.0**. Please, create `subscriber` object "
                "and use it explicitly instead. Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            Union[bool, int],
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: bool = False,
        no_reply: bool = False,
        title: Optional[str] = None,
        description: Optional[str] = None,
        include_in_schema: bool = True,
    ) -> AsyncAPISubscriber:
        """Declares RabbitMQ subscriber object and binds it to the exchange.

        You can use it as a handler decorator - `@broker.subscriber(...)`.
        Or you can create a subscriber object to call it lately - `broker.subscriber(...)`.

        Args:
            queue: RabbitMQ queue to listen. **FastStream** declares and binds
            queue object to `exchange` automatically if it is not passive (by default).
            exchange: RabbitMQ exchange to bind queue to. Uses default exchange
            if not present. **FastStream** declares exchange object automatically
            if it is not passive (by default).
            consume_args: Extra consumer arguments to use in `queue.consume(...)` method.
            channel: Channel to use for consuming messages. If not specified, a default channel will be used.
            reply_config: Extra options to use at replies publishing.
            dependencies: Dependencies list (`[Depends(),]`) to apply to the subscriber.
            parser: Parser to map original **IncomingMessage** Msg to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            filter: Overload subscriber to consume various messages from the same source.
            retry: Whether to `nack` message at processing exception.
            no_ack: Whether to disable **FastStream** autoacknowledgement logic or not.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
        """
        subscriber = cast(
            "AsyncAPISubscriber",
            super().subscriber(
                create_subscriber(
                    queue=RabbitQueue.validate(queue),
                    exchange=RabbitExchange.validate(exchange),
                    consume_args=consume_args,
                    reply_config=reply_config,
                    channel=channel,
                    # subscriber args
                    no_ack=no_ack,
                    no_reply=no_reply,
                    retry=retry,
                    broker_middlewares=self._middlewares,
                    broker_dependencies=self._dependencies,
                    # AsyncAPI
                    title_=title,
                    description_=description,
                    include_in_schema=self._solve_include_in_schema(include_in_schema),
                )
            ),
        )

        return subscriber.add_call(
            filter_=filter,
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        queue: Union["RabbitQueue", str] = "",
        exchange: Union["RabbitExchange", str, None] = None,
        *,
        routing_key: str = "",
        mandatory: bool = True,
        immediate: bool = False,
        timeout: "TimeoutType" = None,
        persist: bool = False,
        reply_to: Optional[str] = None,
        priority: Optional[int] = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        title: Optional[str] = None,
        description: Optional[str] = None,
        schema: Optional[Any] = None,
        include_in_schema: bool = True,
        headers: Optional["HeadersType"] = None,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        expiration: Optional["DateType"] = None,
        message_type: Optional[str] = None,
        user_id: Optional[str] = None,
    ) -> AsyncAPIPublisher:
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.

        Args:
            queue: Default message routing key to publish with.
            exchange: Target exchange to publish message to.
            routing_key: Default message routing key to publish with.
            Overrides `queue` option if presented.
            mandatory: Client waits for confirmation that the message is placed
                to some queue. RabbitMQ returns message to client if there is no suitable queue.
            immediate: Client expects that there is a consumer ready to take the message to work.
                RabbitMQ returns message to client if there is no suitable consumer.
            timeout: Send confirmation time from RabbitMQ.
            persist: Restore the message on RabbitMQ reboot.
            reply_to: Reply message routing key to send with (always sending to default exchange).
            priority: The message priority (0 by default).
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type. Should be any python-native
                object annotation or `pydantic.BaseModel`.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
            headers: Message headers to store meta-information. Can be overridden
                by `publish.headers` if specified.
            content_type: Message **content-type** header. Used by application, not core RabbitMQ.
                Will be set automatically if not specified.
            content_encoding: Message body content encoding, e.g. **gzip**.
            expiration: Message expiration (lifetime) in seconds (or datetime or timedelta).
            message_type: Application-specific message type, e.g. **orders.created**.
            user_id: Publisher connection User ID, validated if set.
        """
        message_kwargs = PublishKwargs(
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
            persist=persist,
            reply_to=reply_to,
            headers=headers,
            priority=priority,
            content_type=content_type,
            content_encoding=content_encoding,
            message_type=message_type,
            user_id=user_id,
            expiration=expiration,
        )

        publisher = cast(
            "AsyncAPIPublisher",
            super().publisher(
                AsyncAPIPublisher.create(
                    routing_key=routing_key,
                    queue=RabbitQueue.validate(queue),
                    exchange=RabbitExchange.validate(exchange),
                    message_kwargs=message_kwargs,
                    # Specific
                    broker_middlewares=self._middlewares,
                    middlewares=middlewares,
                    # AsyncAPI
                    title_=title,
                    description_=description,
                    schema_=schema,
                    include_in_schema=self._solve_include_in_schema(include_in_schema),
                )
            ),
        )

        return publisher

    @override
    def include_router(
        self,
        router: "RabbitRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Depends"] = (),
        middlewares: Iterable["BrokerMiddleware[IncomingMessage]"] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        if not isinstance(router, RabbitRegistrator):
            msg = (
                f"Router must be an instance of RabbitRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
