from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Literal, Optional, Union, cast

from nats.aio.msg import Msg
from nats.js import api
from typing_extensions import deprecated, overload, override

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.exceptions import SetupError
from faststream.middlewares import AckPolicy
from faststream.nats.configs import NatsBrokerConfig
from faststream.nats.helpers import StreamBuilder
from faststream.nats.publisher.factory import create_publisher
from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub
from faststream.nats.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.nats.publisher.usecase import LogicPublisher
    from faststream.nats.subscriber.usecases import (
        BatchPullStreamSubscriber,
        ConcurrentCoreSubscriber,
        ConcurrentPullStreamSubscriber,
        ConcurrentPushStreamSubscriber,
        CoreSubscriber,
        KeyValueWatchSubscriber,
        LogicSubscriber,
        ObjStoreWatchSubscriber,
        PullStreamSubscriber,
        PushStreamSubscriber,
    )


class NatsRegistrator(Registrator[Msg, NatsBrokerConfig]):
    """Includable to NatsBroker router."""

    def __init__(self, **kwargs: Any) -> None:
        self._stream_builder = StreamBuilder()

        super().__init__(**kwargs)

    @overload  # type: ignore[override]
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "CoreSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: int = ...,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ConcurrentCoreSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "PushStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: int = ...,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ConcurrentPushStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[True] = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "PullStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Literal[True] = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: int = ...,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ConcurrentPullStreamSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: PullSub = ...,
        kv_watch: None = None,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream"] = ...,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> Union["PullStreamSubscriber", "BatchPullStreamSubscriber"]: ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: Union[str, "KvWatch"] = ...,
        obj_watch: Literal[False] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "KeyValueWatchSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: None = None,
        config: None = None,
        ordered_consumer: Literal[False] = False,
        idle_heartbeat: None = None,
        flow_control: None = None,
        deliver_policy: None = None,
        headers_only: None = None,
        # pull arguments
        pull_sub: Literal[False] = False,
        kv_watch: None = None,
        obj_watch: Union[Literal[True], "ObjWatch"] = ...,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: None = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "ObjStoreWatchSubscriber": ...

    @overload
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Union[bool, "PullSub"] = False,
        kv_watch: Union[str, "KvWatch", None] = None,
        obj_watch: Union[bool, "ObjWatch"] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream", None] = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "LogicSubscriber[Any]": ...

    @override
    def subscriber(
        self,
        subject: str = "",
        queue: str = "",
        pending_msgs_limit: int | None = None,
        pending_bytes_limit: int | None = None,
        # Core arguments
        max_msgs: int = 0,
        # JS arguments
        durable: str | None = None,
        config: Optional["api.ConsumerConfig"] = None,
        ordered_consumer: bool = False,
        idle_heartbeat: float | None = None,
        flow_control: bool | None = None,
        deliver_policy: Optional["api.DeliverPolicy"] = None,
        headers_only: bool | None = None,
        # pull arguments
        pull_sub: Union[bool, "PullSub"] = False,
        kv_watch: Union[str, "KvWatch", None] = None,
        obj_watch: Union[bool, "ObjWatch"] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        # custom
        stream: Union[str, "JStream", None] = None,
        # broker arguments
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        persistent: bool = True,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
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
        max_workers: int | None = None,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "LogicSubscriber[Any]":
        """Creates NATS subscriber object.

        You can use it as a handler decorator `@broker.subscriber(...)`.

        Args:
            subject: NATS subject to subscribe.
            queue: Subscribers' NATS queue name. Subscribers with same queue name will be load balanced by the NATS
                server.
            pending_msgs_limit: Limit of messages, considered by NATS server as possible to be delivered to the
                client without been answered. In case of NATS Core, if that limits exceeds, you will receive NATS
                'Slow Consumer' error. That's literally means that your worker can't handle the whole load. In case of
                NATS JetStream, you will no longer receive messages until some of delivered messages will be acked in
                any way.
            pending_bytes_limit: The number of bytes, considered by NATS server as possible to be delivered to the
                client without been answered. In case of NATS Core, if that limit exceeds, you will receive NATS 'Slow
                Consumer' error. That's literally means that your worker can't handle the whole load. In case of NATS
                JetStream, you will no longer receive messages until some of delivered messages will be acked in any
                way.
            max_msgs: Consuming messages limiter. Automatically disconnect if reached.
            durable: Name of the durable consumer to which the the subscription should be bound.
            config: Configuration of JetStream consumer to be subscribed with.
            ordered_consumer: Enable ordered consumer mode.
            idle_heartbeat: Enable Heartbeats for a consumer to detect failures.
            flow_control: Enable Flow Control for a consumer.
            deliver_policy: Deliver Policy to be used for subscription.
            headers_only: Should be message delivered without payload, only headers and metadata.
            pull_sub: NATS Pull consumer parameters container. Should be used with `stream` only.
            kv_watch: KeyValue watch parameters container.
            obj_watch: ObjectStore watch parameters container.
            inbox_prefix: Prefix for generating unique inboxes, subjects with that prefix and NUID.
            ack_first: Whether to `ack` message at start of consuming or not.
            stream: Subscribe to NATS Stream with `subject` filter.
            dependencies: Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser: Parser to map original **nats-py** Msg to FastStream one.
            decoder: Function to decode FastStream msg bytes body to python objects.
            middlewares: Subscriber middlewares to wrap incoming message processing.
            max_workers: Number of workers to process messages concurrently.
            no_ack: Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy: Whether to `ack` message at start of consuming or not.
            no_reply: Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description. Uses decorated docstring as default.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
            persistent: Whether to make the subscriber persistent or not.

        Returns:
            LogicSubscriber[Any]: The created subscriber object.
        """
        stream = self._stream_builder.create(stream)

        subscriber = create_subscriber(
            subject=subject,
            queue=queue,
            stream=stream,
            pull_sub=PullSub.validate(pull_sub),
            kv_watch=KvWatch.validate(kv_watch),
            obj_watch=ObjWatch.validate(obj_watch),
            max_workers=max_workers or 1,
            # extra args
            pending_msgs_limit=pending_msgs_limit,
            pending_bytes_limit=pending_bytes_limit,
            max_msgs=max_msgs,
            durable=durable,
            config=config,
            ordered_consumer=ordered_consumer,
            idle_heartbeat=idle_heartbeat,
            flow_control=flow_control,
            deliver_policy=deliver_policy,
            headers_only=headers_only,
            inbox_prefix=inbox_prefix,
            ack_first=ack_first,
            # subscriber args
            ack_policy=ack_policy,
            no_ack=no_ack,
            no_reply=no_reply,
            broker_config=cast("NatsBrokerConfig", self.config),
            # AsyncAPI
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        self._stream_builder.add_subject(stream, subscriber.subject)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        subject: str,
        *,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        stream: Union[str, "JStream", None] = None,
        timeout: float | None = None,
        persistent: bool = True,
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
        ] = (),
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
            subject: NATS subject to send message.
            headers: Message headers to store metainformation.
                content-type and correlation_id will be set automatically by framework anyway.
                Can be overridden by `publish.headers` if specified.
            reply_to: NATS subject name to send response.
            stream: This option validates that the target `subject` is in presented stream.
                Can be omitted without any effect.
            timeout: Timeout to send message to NATS.
            middlewares: Publisher middlewares to wrap outgoing messages.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema: Whetever to include operation in AsyncAPI schema or not.
            persistent: Whether to make the publisher persistent or not.
        """
        stream = self._stream_builder.create(stream)

        publisher = create_publisher(
            subject=subject,
            headers=headers,
            # Core
            reply_to=reply_to,
            # JS
            timeout=timeout,
            stream=stream,
            # Specific
            broker_config=cast("NatsBrokerConfig", self.config),
            middlewares=middlewares,
            # AsyncAPI
            title_=title,
            description_=description,
            schema_=schema,
            include_in_schema=include_in_schema,
        )

        super().publisher(publisher, persistent=persistent)

        self._stream_builder.add_subject(stream, publisher.subject)

        return publisher

    @override
    def include_router(  # type: ignore[override]
        self,
        router: "NatsRegistrator",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, NatsRegistrator):
            msg = (
                f"Router must be an instance of NatsRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        for stream, subjects in router._stream_builder.objects.values():
            for subj in subjects:
                router_subject = f"{self.config.prefix}{prefix}{subj}"
                self._stream_builder.add_subject(stream, router_subject)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
