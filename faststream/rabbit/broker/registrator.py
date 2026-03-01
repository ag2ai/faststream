from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

from aio_pika import IncomingMessage
from typing_extensions import deprecated, override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.rabbit.configs import RabbitBrokerConfig
from faststream.rabbit.publisher.factory import create_publisher
from faststream.rabbit.publisher.options import PublishKwargs
from faststream.rabbit.schemas import (
    Channel,
    RabbitExchange,
    RabbitQueue,
)
from faststream.rabbit.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType, TimeoutType
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.rabbit.publisher import RabbitPublisher
    from faststream.rabbit.subscriber import RabbitSubscriber


class RabbitRegistrator(Registrator[IncomingMessage, RabbitBrokerConfig]):
    """Includable to RabbitBroker router."""

    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: Union[str, "RabbitQueue"],
        exchange: Union[str, "RabbitExchange", None] = None,
        *,
        channel: Optional["Channel"] = None,
        consume_args: dict[str, Any] | None = None,
        no_ack: Annotated[
            bool,
            deprecated(
                "Deprecated in 0.6.0, use `ack_policy=AckPolicy.MANUAL` instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[Any]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        no_reply: bool = False,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "RabbitSubscriber":
        """Subscribe a handler to a RabbitMQ queue.

        Args:
            queue (Union[str, RabbitQueue]): RabbitMQ queue to listen. **FastStream** declares and binds queue object to `exchange` automatically by default.
            exchange (Union[str, RabbitExchange, None], optional): RabbitMQ exchange to bind queue to. Uses default exchange if not presented. **FastStream** declares exchange object automatically by default.
            channel (Optional[Channel], optional): Channel to use for consuming messages.
            consume_args (dict[str, Any] | None, optional): Extra consumer arguments to use in `queue.consume(...)` method.
            no_ack (bool, optional): Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy (AckPolicy, optional): Acknowledgement policy for message processing.
            dependencies (Iterable[Dependant], optional): Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser (Optional[CustomCallable], optional): Parser to map original **IncomingMessage** Msg to FastStream one.
            decoder (Optional[CustomCallable], optional): Function to decode FastStream msg bytes body to python objects.
            middlewares (Sequence[SubscriberMiddleware[Any]], optional): Subscriber middlewares to wrap incoming message processing.
            no_reply (bool, optional): Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title (Optional[str], optional): AsyncAPI subscriber object title.
            description (Optional[str], optional): AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema (bool, optional): Whether to include operation in AsyncAPI schema or not.
            persistent (bool): Whether to make the subscriber persistent or not.

        Returns:
            RabbitSubscriber: The subscriber specification object.
        """
        subscriber = create_subscriber(
            queue=RabbitQueue.validate(queue),
            exchange=RabbitExchange.validate(exchange),
            consume_args=consume_args,
            channel=channel,
            # subscriber args
            ack_policy=ack_policy,
            no_ack=no_ack,
            no_reply=no_reply,
            # broker args
            config=cast("RabbitBrokerConfig", self.config),
            # specification args
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
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
        reply_to: str | None = None,
        priority: int | None = None,
        persistent: bool = True,
        # specific
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
        # message args
        headers: Optional["HeadersType"] = None,
        content_type: str | None = None,
        content_encoding: str | None = None,
        expiration: Optional["DateType"] = None,
        message_type: str | None = None,
        user_id: str | None = None,
    ) -> "RabbitPublisher":
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
            persistent: Whether to make the publisher persistent or not.
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

        publisher = create_publisher(
            routing_key=routing_key,
            queue=RabbitQueue.validate(queue),
            exchange=RabbitExchange.validate(exchange),
            message_kwargs=message_kwargs,
            # publisher args
            middlewares=middlewares,
            # broker args
            config=cast("RabbitBrokerConfig", self.config),
            # specification args
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher, persistent=persistent)

        return publisher

    @override
    def include_router(
        self,
        router: "RabbitRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
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
