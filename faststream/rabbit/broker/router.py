from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union

from aio_pika import IncomingMessage
from typing_extensions import deprecated

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.rabbit.configs import RabbitBrokerConfig

from .registrator import RabbitRegistrator

if TYPE_CHECKING:
    from aio_pika.abc import DateType, HeadersType, TimeoutType
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.rabbit.schemas import (
        RabbitExchange,
        RabbitQueue,
    )
    from faststream.rabbit.types import AioPikaSendableMessage


class RabbitPublisher(ArgsContainer):
    """Delayed RabbitPublisher registration object.

    Just a copy of `RabbitRegistrator.publisher(...)` arguments.
    """

    def __init__(
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
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        # AsyncAPI args
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
    ) -> None:
        """Initialized RabbitPublisher.

        Args:
            queue:
                Default message routing key to publish with.
                Can be any `RabbitQueue` instance or string representation of the queue.
            exchange:
                Target exchange to publish message to.
                Any `RabbitExchange` instance or string representation of the exchange. If not
                specified, it will default to the value provided in `queue`.
            routing_key:
                Default message routing key to publish with. Overrides `queue`
                option if presented.
            mandatory:
                Client waits for confirmation that the message is placed to some queue.
                RabbitMQ returns message to client if there is no suitable queue.
            immediate:
                Client expects that there is consumer ready to take the message to work.
                RabbitMQ returns message to client if there is no suitable consumer.
            timeout:
                Send confirmation time from RabbitMQ.
            persist:
                Restore the message on RabbitMQ reboot.
            reply_to:
                Reply message routing key to send with (always sending to default exchange).
            priority:
                The message priority (0 by default).
            middlewares:
                Publisher middlewares to wrap outgoing messages.
            title:
                AsyncAPI publisher object title.
            description:
                AsyncAPI publisher object description.
            schema:
                AsyncAPI publishing message type. Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
            headers:
                Message headers to store metainformation. Can be overridden by `publish.headers` if specified.
            content_type:
                Message **content-type** header. Used by application, not core RabbitMQ. Will be set automatically if not specified.
            content_encoding:
                Message body content encoding, e.g. **gzip**.
            expiration:
                Message expiration (lifetime) in seconds (or datetime or timedelta).
            message_type:
                Application-specific message type, e.g. **orders.created**.
            user_id:
                Publisher connection User ID, validated if set.
        """
        super().__init__(
            queue=queue,
            exchange=exchange,
            routing_key=routing_key,
            mandatory=mandatory,
            immediate=immediate,
            timeout=timeout,
            persist=persist,
            reply_to=reply_to,
            priority=priority,
            headers=headers,
            content_type=content_type,
            content_encoding=content_encoding,
            expiration=expiration,
            message_type=message_type,
            user_id=user_id,
            # basic args
            middlewares=middlewares,
            # AsyncAPI args
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class RabbitRoute(SubscriberRoute):
    """Class to store delayed RabbitBroker subscriber registration.

    Just a copy of `RabbitRegistrator.subscriber(...)` arguments.
    """

    def __init__(
        self,
        call: Callable[..., "AioPikaSendableMessage"]
        | Callable[..., Awaitable["AioPikaSendableMessage"]],
        queue: Union[str, "RabbitQueue"],
        exchange: Union[str, "RabbitExchange", None] = None,
        *,
        publishers: Iterable[RabbitPublisher] = (),
        consume_args: dict[str, Any] | None = None,
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
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialized RabbitRoute.

        Args:
            call:
                Message handler function
                to wrap the same with `@broker.subscriber(...)` way.
            queue:
                RabbitMQ queue to listen.
                **FastStream** declares and binds queue object to `exchange` automatically by default.
            exchange:
                RabbitMQ exchange to bind queue to.
                Uses default exchange if not present.
                **FastStream** declares exchange object automatically by default.
            publishers:
                RabbitMQ publishers to broadcast the handler result.
            consume_args:
                Extra consumer arguments to use in `queue.consume(...)` method.
            dependencies:
                Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser:
                Parser to map original **IncomingMessage** Msg to FastStream one.
            decoder:
                Function to decode FastStream msg bytes body to python objects.
            middlewares:
                Subscriber middlewares to wrap incoming message processing.
            no_ack:
                Whether to disable **FastStream** auto acknowledgement logic or not.
                Scheduled to remove in 0.7.0
            ack_policy:
                Acknowledgment policy for the subscriber (by default `MANUAL`).
            no_reply:
                Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title:
                AsyncAPI subscriber object title.
            description:
                AsyncAPI subscriber object description.
                Uses decorated docstring as default.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            call,
            publishers=publishers,
            queue=queue,
            exchange=exchange,
            consume_args=consume_args,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            ack_policy=ack_policy,
            no_ack=no_ack,
            no_reply=no_reply,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class RabbitRouter(RabbitRegistrator, BrokerRouter[IncomingMessage]):
    """Includable to RabbitBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[RabbitRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[RabbitRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialized RabbitRouter.

        Args:
            prefix:
                String prefix to add to all subscribers queues.
            handlers:
                Route object to include.
            dependencies:
                Dependencies list (`[Dependant(),]`) to apply to all routers' publishers/subscribers. Defaults to ().
            middlewares:
                Router middlewares to apply to all routers' publishers/subscribers. Defaults to ().
            routers:
                Routers to apply to broker. Defaults to ().
            parser:
                Parser to map original **IncomingMessage** Msg to FastStream one. Defaults to None.
            decoder:
                Function to decode FastStream msg bytes body to python objects. Defaults to None.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not. Defaults to None.
        """
        super().__init__(
            handlers=handlers,
            config=RabbitBrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )
