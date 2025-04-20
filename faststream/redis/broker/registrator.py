from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

from typing_extensions import Doc, deprecated, override

from faststream._internal.broker.abc_broker import ABCBroker
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.redis.message import UnifyRedisDict
from faststream.redis.publisher.factory import create_publisher
from faststream.redis.subscriber.factory import SubsciberType, create_subscriber
from faststream.redis.subscriber.specified import (
    SpecificationConcurrentSubscriber,
    SpecificationSubscriber,
)

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.redis.message import UnifyRedisMessage
    from faststream.redis.publisher.specified import (
        PublisherType,
        SpecificationPublisher,
    )
    from faststream.redis.schemas import ListSub, PubSub, StreamSub


class RedisRegistrator(ABCBroker[UnifyRedisDict]):
    """Includable to RedisBroker router."""

    _subscribers: list["SubsciberType"]
    _publishers: list["PublisherType"]

    @override
    def subscriber(  # type: ignore[override]
        self,
        channel: Annotated[
            Union["PubSub", str, None],
            Doc("Redis PubSub object name to send message."),
        ] = None,
        *,
        list: Annotated[
            Union["ListSub", str, None],
            Doc("Redis List object name to send message."),
        ] = None,
        stream: Annotated[
            Union["StreamSub", str, None],
            Doc("Redis Stream object name to send message."),
        ] = None,
        # broker arguments
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc("Dependencies list (`[Dependant(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc(
                "Parser to map original **aio_pika.IncomingMessage** Msg to FastStream one.",
            ),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[UnifyRedisMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0"
            ),
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** auto acknowledgement logic or not."),
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.DO_NOTHING**. "
                "Scheduled to remove in 0.7.0"
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not.",
            ),
        ] = False,
        # AsyncAPI information
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default.",
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
        max_workers: Annotated[
            int,
            Doc("Number of workers to process messages concurrently."),
        ] = 1,
    ) -> Union[SpecificationSubscriber, SpecificationConcurrentSubscriber]:
        subscriber = create_subscriber(
            channel=channel,
            list=list,
            stream=stream,
            # subscriber args
            max_workers=max_workers,
            no_ack=no_ack,
            no_reply=no_reply,
            ack_policy=ack_policy,
            broker_middlewares=self.middlewares,
            broker_dependencies=self._dependencies,
            # AsyncAPI
            title_=title,
            description_=description,
            include_in_schema=self._solve_include_in_schema(include_in_schema),
        )

        if max_workers > 1:
            subscriber = cast("SpecificationConcurrentSubscriber", subscriber)
        else:
            subscriber = cast("SpecificationSubscriber", subscriber)

        subscriber = super().subscriber(subscriber)  # type: ignore[assignment]

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        channel: Annotated[
            Union["PubSub", str, None],
            Doc("Redis PubSub object name to send message."),
        ] = None,
        *,
        list: Annotated[
            Union["ListSub", str, None],
            Doc("Redis List object name to send message."),
        ] = None,
        stream: Annotated[
            Union["StreamSub", str, None],
            Doc("Redis Stream object name to send message."),
        ] = None,
        headers: Annotated[
            Optional["AnyDict"],
            Doc(
                "Message headers to store metainformation. "
                "Can be overridden by `publish.headers` if specified.",
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0"
            ),
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI information
        title: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            Optional[str],
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Optional[Any],
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`.",
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> "SpecificationPublisher":
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.
        """
        return cast(
            "SpecificationPublisher",
            super().publisher(
                create_publisher(
                    channel=channel,
                    list=list,
                    stream=stream,
                    headers=headers,
                    reply_to=reply_to,
                    # Specific
                    broker_middlewares=self.middlewares,
                    middlewares=middlewares,
                    # AsyncAPI
                    title_=title,
                    description_=description,
                    schema_=schema,
                    include_in_schema=self._solve_include_in_schema(include_in_schema),
                ),
            ),
        )

    @override
    def include_router(
        self,
        router: "RedisRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Iterable["BrokerMiddleware[UnifyRedisDict]"] = (),
        include_in_schema: Optional[bool] = None,
    ) -> None:
        if not isinstance(router, RedisRegistrator):
            msg = (
                f"Router must be an instance of RedisRegistrator, "
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
