from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, cast

from typing_extensions import override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.mq.configs import MQBrokerConfig
from faststream.mq.publisher.factory import create_publisher
from faststream.mq.schemas import MQQueue
from faststream.mq.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.mq.publisher import MQPublisher
    from faststream.mq.subscriber import MQSubscriber


class MQRegistrator(Registrator[Any, MQBrokerConfig]):
    @override
    def subscriber(  # type: ignore[override]
        self,
        queue: MQQueue | str,
        *,
        ack_policy: AckPolicy = EMPTY,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        middlewares: Sequence["SubscriberMiddleware[Any]"] = (),
        no_reply: bool = False,
        wait_interval: float = 1.0,
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "MQSubscriber":
        subscriber = create_subscriber(
            queue=MQQueue.validate(queue),
            no_reply=no_reply,
            ack_policy=ack_policy,
            wait_interval=wait_interval,
            config=cast(MQBrokerConfig, self.config),
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
        queue: MQQueue | str,
        *,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        reply_to_qmgr: str = "",
        priority: int | None = None,
        persistence: bool | None = None,
        expiry: int | None = None,
        message_type: str | None = None,
        middlewares: Sequence["PublisherMiddleware"] = (),
        persistent: bool = True,
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "MQPublisher":
        publisher = create_publisher(
            queue=MQQueue.validate(queue),
            config=cast(MQBrokerConfig, self.config),
            middlewares=middlewares,
            schema_=schema,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
            headers=headers,
            reply_to=reply_to,
            reply_to_qmgr=reply_to_qmgr,
            priority=priority,
            persistence=persistence,
            expiry=expiry,
            message_type=message_type,
        )
        super().publisher(publisher, persistent=persistent)
        return publisher

    @override
    def include_router(
        self,
        router: "MQRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, MQRegistrator):
            msg = (
                f"Router must be an instance of MQRegistrator, "
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
