from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.mq.configs import MQBrokerConfig

from .registrator import MQRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.mq.schemas import MQQueue


class MQPublisher(ArgsContainer):
    def __init__(
        self,
        queue: "MQQueue | str",
        *,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            queue=queue,
            headers=headers,
            reply_to=reply_to,
            reply_to_qmgr=reply_to_qmgr,
            priority=priority,
            persistence=persistence,
            expiry=expiry,
            message_type=message_type,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class MQRoute(SubscriberRoute):
    def __init__(
        self,
        call: Callable[..., Any] | Callable[..., Awaitable[Any]],
        queue: "MQQueue | str",
        *,
        publishers: Iterable[MQPublisher] = (),
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        wait_interval: float = 1.0,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call,
            publishers=publishers,
            queue=queue,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            middlewares=middlewares,
            ack_policy=ack_policy,
            no_reply=no_reply,
            wait_interval=wait_interval,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class MQRouter(MQRegistrator, BrokerRouter[Any]):
    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[MQRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[MQRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            handlers=handlers,
            routers=routers,
            config=MQBrokerConfig(
                prefix=prefix,
                include_in_schema=include_in_schema,
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
            ),
        )
