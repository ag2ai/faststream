from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.redis.configs.broker import RedisRouterConfig
from faststream.redis.message import BaseMessage

from .registrator import RedisRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.redis.schemas import ListSub, PubSub, StreamSub


class RedisPublisher(ArgsContainer):
    """Delayed RedisPublisher registration object.

    Just a copy of RedisRegistrator.publisher(...) arguments.
    """

    def __init__(
        self,
        channel: str | None = None,
        *,
        list: str | None = None,
        stream: str | None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Sequence["PublisherMiddleware"] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize the RedisPublisher.

        Args:
            channel:
                Redis PubSub object name to send message.
            list:
                Redis List object name to send message.
            stream:
                Redis Stream object name to send message.
            headers:
                Message headers to store metainformation. Can be overridden by `publish.headers` if specified.
            reply_to:
                Reply message destination PubSub object name.
            middlewares:
                Publisher middlewares to wrap outgoing messages.
            title:
                AsyncAPI publisher object title.
            description:
                AsyncAPI publisher object description.
            schema:
                AsyncAPI publishing message type.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.

        """
        super().__init__(
            channel=channel,
            list=list,
            stream=stream,
            headers=headers,
            reply_to=reply_to,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class RedisRoute(SubscriberRoute):
    """Class to store delayed RedisBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        channel: Union[str, "PubSub"] | None = None,
        *,
        publishers: Iterable["RedisPublisher"] = (),
        list: Union[str, "ListSub"] | None = None,
        stream: Union[str, "StreamSub"] | None = None,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        no_ack: bool = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> None:
        """Initialize the RedisRoute.

        Args:
            call:
                Message handler function to wrap the same with `@broker.subscriber(...)` way.
            channel:
                Redis PubSub object name to send message.
            publishers:
                Redis publishers to broadcast the handler result.
            list:
                Redis List object name to send message.
            stream:
                Redis Stream object name to send message.
            dependencies:
                Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser:
                Parser to map original **aio_pika.IncomingMessage** Msg to FastStream one.
            decoder:
                Function to decode FastStream msg bytes body to python objects.
            middlewares:
                Subscriber middlewares to wrap incoming message processing. (Deprecated)
            no_ack:
                Whether to disable **FastStream** auto acknowledgement logic or not. (Deprecated)
            ack_policy:
                Acknowledgement policy of the handler.
            no_reply:
                Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title:
                AsyncAPI subscriber object title.
            description:
                AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
            max_workers:
                Number of workers to process messages concurrently.
        """
        super().__init__(
            call,
            channel=channel,
            publishers=publishers,
            list=list,
            stream=stream,
            dependencies=dependencies,
            max_workers=max_workers,
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


class RedisRouter(
    RedisRegistrator,
    BrokerRouter[BaseMessage],
):
    """Includable to RedisBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[RedisRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[RedisRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialize the RedisRouter.

        Args:
            prefix:
                String prefix to add to all subscribers queues.
            handlers:
                Route object to include.
            dependencies:
                Dependencies list (`[Dependant(),]`) to apply to all routers' publishers/subscribers.
            middlewares:
                Router middlewares to apply to all routers' publishers/subscribers.
            routers:
                Routers to apply to broker.
            parser:
                Parser to map original **IncomingMessage** Msg to FastStream one.
            decoder:
                Function to decode FastStream msg bytes body to python objects.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            handlers=handlers,
            config=RedisRouterConfig(
                prefix=prefix,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
            ),
            routers=routers,
        )
