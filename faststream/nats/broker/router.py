from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union

from nats.aio.msg import Msg
from nats.js import api
from typing_extensions import deprecated

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.nats.configs import NatsBrokerConfig

from .registrator import NatsRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
        PublisherMiddleware,
        SubscriberMiddleware,
    )
    from faststream.nats.schemas import JStream, KvWatch, ObjWatch, PullSub


class NatsPublisher(ArgsContainer):
    """Delayed NatsPublisher registration object.

    Just a copy of `KafkaRegistrator.publisher(...)` arguments.
    """

    def __init__(
        self,
        subject: str = "",
        *,
        headers: dict[str, str] | None = None,
        reply_to: str = "",
        # JS
        stream: Union[str, "JStream", None] = None,
        timeout: float | None = None,
        # basic args
        middlewares: Sequence["PublisherMiddleware"] = (),
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialized the NatsPublisher object.

        Args:
            subject:
                NATS subject to send message.
            headers:
                Message headers to store metainformation.
                **content-type** and **correlation_id** will be set automatically by framework anyway. Can be overridden by `publish.headers` if specified.
            reply_to:
                NATS subject name to send response.
            stream:
                This option validates that the target `subject` is in presented stream.
                Can be omitted without any effect.
            timeout:
                Timeout to send message to NATS.
            middlewares:
                Publisher middlewares to wrap outgoing messages.
            title:
                AsyncAPI publisher object title.
            description:
                AsyncAPI publisher object description.
            schema:
                AsyncAPI publishing message type.
                Should be any python-native object annotation or `pydantic.BaseModel`.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            subject=subject,
            headers=headers,
            reply_to=reply_to,
            stream=stream,
            timeout=timeout,
            middlewares=middlewares,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class NatsRoute(SubscriberRoute):
    """Class to store delayed NatsBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        subject: str,
        publishers: Iterable[NatsPublisher] = (),
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
        pull_sub: Optional["PullSub"] = None,
        kv_watch: Union[str, "KvWatch", None] = None,
        obj_watch: Union[bool, "ObjWatch"] = False,
        inbox_prefix: bytes = api.INBOX_PREFIX,
        ack_first: Annotated[
            bool,
            deprecated(
                "This option is deprecated and will be removed in 0.7.0 release. "
                "Please, use `ack_policy=AckPolicy.ACK_FIRST` instead."
            ),
        ] = EMPTY,
        stream: Union[str, "JStream", None] = None,
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
        max_workers: int | None = None,
        no_ack: Annotated[
            bool,
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.MANUAL**. "
                "Scheduled to remove in 0.7.0",
            ),
        ] = EMPTY,
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialized NatsRoute.

        Args:
            call:
                Message handler function to wrap the same with `@broker.subscriber(...)` way.
            subject:
                NATS subject to subscribe.
            publishers:
                Nats publishers to broadcast the handler result.
            queue:
                Subscribers' NATS queue name. Subscribers with same queue name will be load balanced by the NATS server.
            pending_msgs_limit:
                Limit of messages, considered by NATS server as possible to be delivered to the client without been answered.
            pending_bytes_limit:
                The number of bytes, considered by NATS server as possible to be delivered to the client without been answered.
            max_msgs:
                Consuming messages limiter. Automatically disconnect if reached.
            durable:
                Name of the durable consumer to which the the subscription should be bound.
            config:
                Configuration of JetStream consumer to be subscribed with.
            ordered_consumer:
                Enable ordered consumer mode.
            idle_heartbeat:
                Enable Heartbeats for a consumer to detect failures.
            flow_control:
                Enable Flow Control for a consumer.
            deliver_policy:
                Deliver Policy to be used for subscription.
            headers_only:
                Should be message delivered without payload, only headers and metadata.
            pull_sub:
                NATS Pull consumer parameters container. Should be used with `stream` only.
            kv_watch:
                KeyValue watch parameters container.
            obj_watch:
                ObjectStore watch parameters container.
            inbox_prefix:
                Prefix for generating unique inboxes, subjects with that prefix and NUID.
            ack_first:
                Whether to `ack` message at start of consuming or not.
            stream:
                Subscribe to NATS Stream with `subject` filter.
            dependencies:
                Dependencies list (`[Dependant(),]`) to apply to the subscriber.
            parser:
                Parser to map original **nats-py** Msg to FastStream one.
            decoder:
                Function to decode FastStream msg bytes body to python objects.
            middlewares:
                Subscriber middlewares to wrap incoming message processing.
            max_workers:
                Number of workers to process messages concurrently.
            no_ack:
                Whether to disable **FastStream** auto acknowledgement logic or not.
            ack_policy:
                Acknowledgment policy for subscriber.
            no_reply:
                Whether to disable **FastStream** RPC and Reply To auto responses or not.
            title:
                AsyncAPI subscriber object title.
            description:
                AsyncAPI subscriber object description.
                "Uses decorated docstring as default.
            include_in_schema:
                Whetever to include operation in AsyncAPI schema or not.
        """
        super().__init__(
            call,
            subject=subject,
            publishers=publishers,
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
            pull_sub=pull_sub,
            kv_watch=kv_watch,
            obj_watch=obj_watch,
            inbox_prefix=inbox_prefix,
            ack_first=ack_first,
            stream=stream,
            max_workers=max_workers,
            queue=queue,
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


class NatsRouter(NatsRegistrator, BrokerRouter[Msg]):
    """Includable to NatsBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[NatsRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[NatsRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
    ) -> None:
        """Initialize the NatsRouter instance.

        Args:
            prefix:
                String prefix to add to all subscribers subjects. Defaults to "".
            handlers:
                Route object to include. Defaults to ().
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
            config=NatsBrokerConfig(
                broker_middlewares=middlewares,
                broker_dependencies=dependencies,
                broker_parser=parser,
                broker_decoder=decoder,
                include_in_schema=include_in_schema,
                prefix=prefix,
            ),
            routers=routers,
        )
