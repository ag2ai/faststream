from typing import TYPE_CHECKING, Any, Dict, Iterable, Optional, Sequence, Union, cast

from typing_extensions import Annotated, Doc, deprecated, override

from faststream.broker.core.abc import ABCBroker
from faststream.broker.utils import default_filter
from faststream.exceptions import SetupError
from faststream.redis.message import UnifyRedisDict
from faststream.redis.publisher.asyncapi import AsyncAPIPublisher
from faststream.redis.subscriber.asyncapi import AsyncAPISubscriber
from faststream.redis.subscriber.factory import SubsciberType, create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.types import (
        BrokerMiddleware,
        CustomCallable,
        Filter,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.redis.message import UnifyRedisMessage
    from faststream.redis.publisher.asyncapi import PublisherType
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.types import AnyDict


class RedisRegistrator(ABCBroker[UnifyRedisDict]):
    """Includable to RedisBroker router."""

    _subscribers: Dict[int, "SubsciberType"]
    _publishers: Dict[int, "PublisherType"]

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
            Iterable["Depends"],
            Doc("Dependencies list (`[Depends(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc(
                "Parser to map original **aio_pika.IncomingMessage** Msg to FastStream one."
            ),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[UnifyRedisMessage]"],
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        filter: Annotated[
            "Filter[UnifyRedisMessage]",
            Doc(
                "Overload subscriber to consume various messages from the same source."
            ),
            deprecated(
                "Deprecated in **FastStream 0.5.0**. "
                "Please, create `subscriber` object and use it explicitly instead. "
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = default_filter,
        retry: Annotated[
            bool,
            Doc("Whether to `nack` message at processing exception."),
            deprecated(
                "Deprecated in **FastStream 0.5.40**."
                "Please, manage acknowledgement policy manually."
                "Argument will be removed in **FastStream 0.6.0**."
            ),
        ] = False,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** autoacknowledgement logic or not."),
        ] = False,
        no_reply: Annotated[
            bool,
            Doc(
                "Whether to disable **FastStream** RPC and Reply To auto responses or not."
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
                "Uses decorated docstring as default."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> AsyncAPISubscriber:
        subscriber = cast(
            "AsyncAPISubscriber",
            super().subscriber(
                create_subscriber(
                    channel=channel,
                    list=list,
                    stream=stream,
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
                "Can be overridden by `publish.headers` if specified."
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("Reply message destination PubSub object name."),
        ] = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
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
                "Should be any python-native object annotation or `pydantic.BaseModel`."
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> AsyncAPIPublisher:
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.
        """
        return cast(
            "AsyncAPIPublisher",
            super().publisher(
                AsyncAPIPublisher.create(
                    channel=channel,
                    list=list,
                    stream=stream,
                    headers=headers,
                    reply_to=reply_to,
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

    @override
    def include_router(
        self,
        router: "RedisRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Depends"] = (),
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
