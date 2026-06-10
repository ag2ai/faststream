from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, Union

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.sqs.configs import SQSBrokerConfig

from .registrator import SQSRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable
    from faststream.sqs.schemas import SQSQueue


class SQSPublisher(ArgsContainer):
    """Delayed SQSPublisher registration object for use in SQSRoute."""

    def __init__(
        self,
        queue: Union[str, "SQSQueue"],
        *,
        headers: dict[str, str] | None = None,
        group_id: str | None = None,
        deduplication_id: str | None = None,
        delay_seconds: int = 0,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            queue,
            headers=headers,
            group_id=group_id,
            deduplication_id=deduplication_id,
            delay_seconds=delay_seconds,
            persistent=persistent,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class SQSRoute(SubscriberRoute):
    """Class to store a delayed SQSBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        queue: Union[str, "SQSQueue"],
        *,
        publishers: Iterable["SQSPublisher"] = (),
        wait_time_seconds: int = 20,
        max_messages: int = 10,
        visibility_timeout: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call,
            queue,
            publishers=publishers,
            wait_time_seconds=wait_time_seconds,
            max_messages=max_messages,
            visibility_timeout=visibility_timeout,
            ack_policy=ack_policy,
            no_reply=no_reply,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            persistent=persistent,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class SQSRouter(SQSRegistrator, BrokerRouter["Any"]):
    """Includable to SQSBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[SQSRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[SQSRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
        ack_policy: AckPolicy = EMPTY,
    ) -> None:
        super().__init__(
            handlers=handlers,
            config=SQSBrokerConfig(
                prefix=prefix,
                ack_policy=ack_policy,
                broker_dependencies=dependencies,
                broker_middlewares=middlewares,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
            ),
            routers=routers,
        )
