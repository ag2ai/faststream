from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Annotated, Any, Optional, Union, cast

from nats.aio.msg import Msg
from nats.js import api
from typing_extensions import Doc, deprecated, override

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
    from faststream.nats.message import NatsMessage
    from faststream.nats.publisher.usecase import LogicPublisher
    from faststream.nats.subscriber.usecases import LogicSubscriber


class NatsRegistrator(Registrator[Msg, NatsBrokerConfig]):
    """Includable to NatsBroker router."""

    def __init__(self, **kwargs: Any) -> None:
        self._stream_builder = StreamBuilder()

        super().__init__(**kwargs)

    @override
    def subscriber(  # type: ignore[override]
        self,
        subject: Annotated[
            str,
            Doc("NATS subject to subscribe."),
        ] = "",
        queue: Annotated[
            str,
            Doc(
                "Subscribers' NATS queue name. Subscribers with same queue name will be load balanced by the NATS "
                "server.",
            ),
        ] = "",
        pending_msgs_limit: Annotated[
            int | None,
            Doc(
                "Limit of messages, considered by NATS server as possible to be delivered to the client without "
                "been answered. In case of NATS Core, if that limits exceeds, you will receive NATS 'Slow Consumer' "
                "error. "
                "That's literally means that your worker can't handle the whole load. In case of NATS JetStream, "
                "you will no longer receive messages until some of delivered messages will be acked in any way.",
            ),
        ] = None,
        pending_bytes_limit: Annotated[
            int | None,
            Doc(
                "The number of bytes, considered by NATS server as possible to be delivered to the client without "
                "been answered. In case of NATS Core, if that limit exceeds, you will receive NATS 'Slow Consumer' "
                "error."
                "That's literally means that your worker can't handle the whole load. In case of NATS JetStream, "
                "you will no longer receive messages until some of delivered messages will be acked in any way.",
            ),
        ] = None,
        # Core arguments
        max_msgs: Annotated[
            int,
            Doc("Consuming messages limiter. Automatically disconnect if reached."),
        ] = 0,
        # JS arguments
        durable: Annotated[
            str | None,
            Doc(
                "Name of the durable consumer to which the the subscription should be bound.",
            ),
        ] = None,
        config: Annotated[
            Optional["api.ConsumerConfig"],
            Doc("Configuration of JetStream consumer to be subscribed with."),
        ] = None,
        ordered_consumer: Annotated[
            bool,
            Doc("Enable ordered consumer mode."),
        ] = False,
        idle_heartbeat: Annotated[
            float | None,
            Doc("Enable Heartbeats for a consumer to detect failures."),
        ] = None,
        flow_control: Annotated[
            bool | None,
            Doc("Enable Flow Control for a consumer."),
        ] = None,
        deliver_policy: Annotated[
            Optional["api.DeliverPolicy"],
            Doc("Deliver Policy to be used for subscription."),
        ] = None,
        headers_only: Annotated[
            bool | None,
            Doc(
                "Should be message delivered without payload, only headers and metadata.",
            ),
        ] = None,
        # pull arguments
        pull_sub: Annotated[
            Union[bool, "PullSub"],
            Doc(
                "NATS Pull consumer parameters container. "
                "Should be used with `stream` only.",
            ),
        ] = False,
        kv_watch: Annotated[
            Union[str, "KvWatch", None],
            Doc("KeyValue watch parameters container."),
        ] = None,
        obj_watch: Annotated[
            Union[bool, "ObjWatch"],
            Doc("ObjectStore watch parameters container."),
        ] = False,
        inbox_prefix: Annotated[
            bytes,
            Doc(
                "Prefix for generating unique inboxes, subjects with that prefix and NUID.",
            ),
        ] = api.INBOX_PREFIX,
        # custom
        ack_first: Annotated[
            bool,
            Doc("Whether to `ack` message at start of consuming or not."),
            deprecated(
                """
            This option is deprecated and will be removed in 0.7.0 release.
            Please, use `ack_policy=AckPolicy.ACK_FIRST` instead.
            """,
            ),
        ] = EMPTY,
        stream: Annotated[
            Union[str, "JStream", None],
            Doc("Subscribe to NATS Stream with `subject` filter."),
        ] = None,
        # broker arguments
        dependencies: Annotated[
            Iterable["Dependant"],
            Doc("Dependencies list (`[Dependant(),]`) to apply to the subscriber."),
        ] = (),
        parser: Annotated[
            Optional["CustomCallable"],
            Doc("Parser to map original **nats-py** Msg to FastStream one."),
        ] = None,
        decoder: Annotated[
            Optional["CustomCallable"],
            Doc("Function to decode FastStream msg bytes body to python objects."),
        ] = None,
        middlewares: Annotated[
            Sequence["SubscriberMiddleware[NatsMessage]"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
            Doc("Subscriber middlewares to wrap incoming message processing."),
        ] = (),
        max_workers: Annotated[
            int,
            Doc("Number of workers to process messages concurrently."),
        ] = 1,
        no_ack: Annotated[
            bool,
            Doc("Whether to disable **FastStream** auto acknowledgement logic or not."),
            deprecated(
                "This option was deprecated in 0.6.0 to prior to **ack_policy=AckPolicy.DO_NOTHING**. "
                "Scheduled to remove in 0.7.0",
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
            str | None,
            Doc("AsyncAPI subscriber object title."),
        ] = None,
        description: Annotated[
            str | None,
            Doc(
                "AsyncAPI subscriber object description. "
                "Uses decorated docstring as default.",
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> "LogicSubscriber[Msg]":
        """Creates NATS subscriber object.

        You can use it as a handler decorator `@broker.subscriber(...)`.
        """
        stream = self._stream_builder.create(stream)

        subscriber = create_subscriber(
            subject=subject,
            queue=queue,
            stream=stream,
            pull_sub=PullSub.validate(pull_sub),
            kv_watch=KvWatch.validate(kv_watch),
            obj_watch=ObjWatch.validate(obj_watch),
            max_workers=max_workers,
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

        super().subscriber(subscriber)

        if stream and subscriber.subject:
            stream.add_subject(subscriber.subject)

        return subscriber.add_call(
            parser_=parser,
            decoder_=decoder,
            dependencies_=dependencies,
            middlewares_=middlewares,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        subject: Annotated[
            str,
            Doc("NATS subject to send message."),
        ],
        *,
        headers: Annotated[
            dict[str, str] | None,
            Doc(
                "Message headers to store metainformation. "
                "**content-type** and **correlation_id** will be set automatically by framework anyway. "
                "Can be overridden by `publish.headers` if specified.",
            ),
        ] = None,
        reply_to: Annotated[
            str,
            Doc("NATS subject name to send response."),
        ] = "",
        # JS
        stream: Annotated[
            Union[str, "JStream", None],
            Doc(
                "This option validates that the target `subject` is in presented stream. "
                "Can be omitted without any effect.",
            ),
        ] = None,
        timeout: Annotated[
            float | None,
            Doc("Timeout to send message to NATS."),
        ] = None,
        # basic args
        middlewares: Annotated[
            Sequence["PublisherMiddleware"],
            deprecated(
                "This option was deprecated in 0.6.0. Use router-level middlewares instead."
                "Scheduled to remove in 0.7.0",
            ),
            Doc("Publisher middlewares to wrap outgoing messages."),
        ] = (),
        # AsyncAPI information
        title: Annotated[
            str | None,
            Doc("AsyncAPI publisher object title."),
        ] = None,
        description: Annotated[
            str | None,
            Doc("AsyncAPI publisher object description."),
        ] = None,
        schema: Annotated[
            Any | None,
            Doc(
                "AsyncAPI publishing message type. "
                "Should be any python-native object annotation or `pydantic.BaseModel`.",
            ),
        ] = None,
        include_in_schema: Annotated[
            bool,
            Doc("Whetever to include operation in AsyncAPI schema or not."),
        ] = True,
    ) -> "LogicPublisher":
        """Creates long-living and AsyncAPI-documented publisher object.

        You can use it as a handler decorator (handler should be decorated by `@broker.subscriber(...)` too) - `@broker.publisher(...)`.
        In such case publisher will publish your handler return value.

        Or you can create a publisher object to call it lately - `broker.publisher(...).publish(...)`.
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

        super().publisher(publisher)

        if stream and publisher.subject:
            stream.add_subject(publisher.subject)

        return publisher

    @override
    def include_router(  # type: ignore[override]
        self,
        router: "NatsRegistrator",
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Msg]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, NatsRegistrator):
            msg = (
                f"Router must be an instance of NatsRegistrator, "
                f"got {type(router).__name__} instead"
            )
            raise SetupError(msg)

        sub_streams = router._stream_builder.objects.copy()

        sub_router_subjects = [sub.subject for sub in router.subscribers]

        for stream in sub_streams.values():
            new_subjects = []
            for subj in stream.subjects:
                if subj in sub_router_subjects:
                    new_subjects.append(f"{self.config.prefix}{subj}")
                else:
                    new_subjects.append(subj)
            stream.subjects = new_subjects

        self._stream_builder.objects.update(sub_streams)

        return super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )
