from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

from typing_extensions import deprecated, overload, override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.redis.configs import RedisBrokerConfig
from faststream.redis.message import UnifyRedisDict
from faststream.redis.publisher.factory import create_publisher
from faststream.redis.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.redis.parser import MessageFormat
    from faststream.redis.publisher.usecase import (
        ChannelPublisher,
        ListBatchPublisher,
        ListPublisher,
        LogicPublisher,
        StreamPublisher,
    )
    from faststream.redis.schemas import ListSub, PubSub, StreamSub
    from faststream.redis.subscriber.usecases import (
        ChannelConcurrentSubscriber,
        ChannelSubscriber,
        ListBatchSubscriber,
        ListConcurrentSubscriber,
        ListSubscriber,
        LogicSubscriber,
        StreamBatchSubscriber,
        StreamConcurrentSubscriber,
        StreamSubscriber,
    )


class RedisRegistrator(Registrator[UnifyRedisDict, RedisBrokerConfig]):
    """Includable to RedisBroker router."""

    @overload  # type: ignore[override]
    def subscriber(
        self,
        channel: Union["PubSub", str] = ...,
        *,
        list: None = None,
        stream: None = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: None = None,
    ) -> "ChannelSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: Union["PubSub", str] = ...,
        *,
        list: None = None,
        stream: None = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int = ...,
    ) -> "ChannelConcurrentSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: str = ...,
        stream: None = None,
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
        message_format: type["MessageFormat"] | None = None,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: None = None,
    ) -> "ListSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: Union["ListSub", str] = ...,
        stream: None = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: None = None,
    ) -> Union["ListSubscriber", "ListBatchSubscriber"]: ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: Union["ListSub", str] = ...,
        stream: None = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int = ...,
    ) -> "ListConcurrentSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: str = ...,
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
                "Scheduled to remove in 0.7.0"
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: None = None,
    ) -> "StreamSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: Union["StreamSub", str] = ...,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: None = None,
    ) -> Union["StreamSubscriber", "StreamBatchSubscriber"]: ...

    @overload
    def subscriber(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: Union["StreamSub", str] = ...,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int = ...,
    ) -> "StreamConcurrentSubscriber": ...

    @overload
    def subscriber(
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> "LogicSubscriber": ...

    @override
    def subscriber(
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
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
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
        max_workers: int | None = None,
    ) -> "LogicSubscriber":
        """Subscribe a handler to a RabbitMQ queue.

        Args:
            channel: Redis PubSub object name to send message.
            list: Redis List object name to send message.
            stream: Redis Stream object name to send message.
            no_ack: Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy: Acknowledgement policy for message processing.
            dependencies: Dependencies list (`[Depends(),]`) to apply to the subscriber.
            parser: Parser to map original **IncomingMessage** Msg to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            message_format: Which format to use when parsing messages.
            persistent: Whether to make the subscriber persistent or not.
            max_workers: Number of workers to process messages concurrently.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.

        Returns:
            SubscriberType: The subscriber object.
        """
        subscriber = create_subscriber(
            channel=channel,
            list=list,
            stream=stream,
            # subscriber args
            max_workers=max_workers or 1,
            no_ack=no_ack,
            no_reply=no_reply,
            ack_policy=ack_policy,
            message_format=message_format,
            config=cast("RedisBrokerConfig", self.config),
            # AsyncAPI
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @overload  # type: ignore[override]
    def publisher(
        self,
        channel: None = None,
        *,
        list: None = None,
        stream: Union["StreamSub", str] = ...,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "StreamPublisher": ...

    @overload
    def publisher(
        self,
        channel: None = None,
        *,
        list: str = ...,
        stream: None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "ListPublisher": ...

    @overload
    def publisher(
        self,
        channel: None = None,
        *,
        list: Union["ListSub", str] = ...,
        stream: None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> Union["ListPublisher", "ListBatchPublisher"]: ...

    @overload
    def publisher(
        self,
        channel: Union["PubSub", str] = ...,
        *,
        list: None = None,
        stream: None = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "ChannelPublisher": ...

    @overload
    def publisher(
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "LogicPublisher": ...

    @override
    def publisher(
        self,
        channel: Union["PubSub", str, None] = None,
        *,
        list: Union["ListSub", str, None] = None,
        stream: Union["StreamSub", str, None] = None,
        headers: dict[str, Any] | None = None,
        reply_to: str = "",
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
        message_format: type["MessageFormat"] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "LogicPublisher":
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.

        Args:
            channel: Redis PubSub object name to send message.
            list: Redis List object name to send message.
            stream: Redis Stream object name to send message.
            headers: Message headers to store meta-information. Can be overridden
                by `publish.headers` if specified.
            reply_to: Reply message destination PubSub object name.
            middlewares: Publisher middlewares to wrap outgoing messages.
            message_format: Which format to use when parsing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type. Should be any python-native
                object annotation or `pydantic.BaseModel`.
            include_in_schema: Whether to include operation in AsyncAPI schema or not.
            persistent: Whether to make the publisher persistent or not.
        """
        publisher = create_publisher(
            channel=channel,
            list=list,
            stream=stream,
            headers=headers,
            reply_to=reply_to,
            # Specific
            config=cast("RedisBrokerConfig", self.config),
            middlewares=middlewares,
            message_format=message_format,
            # AsyncAPI
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
        router: "RedisRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
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
