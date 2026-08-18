from collections.abc import Awaitable, Callable, Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional

from zmqtt import QoS

from faststream._internal.broker.router import (
    ArgsContainer,
    BrokerRouter,
    SubscriberRoute,
)
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.mqtt.broker.config import MQTTBrokerConfig

from .registrator import MQTTRegistrator

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import SendableMessage
    from faststream._internal.types import BrokerMiddleware, CustomCallable


class MQTTPublisher(ArgsContainer):
    """Delayed MQTTPublisher registration object.

    A copy of MQTTRegistrator.publisher(...) arguments for use in MQTTRoute.
    """

    def __init__(
        self,
        topic: str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
        headers: dict[str, str] | None = None,
        skip_none: bool = False,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> None:
        """Initialize MQTTPublisher.

        Args:
            topic: MQTT topic to publish to. Must not contain wildcards.
            qos: QoS level for published messages (0, 1, or 2).
            retain: Whether the broker should retain the last message.
            headers: Default headers to include in every published message.
            skip_none:
                Whether to skip publishing `None` message values or not.
                A returned `None` is not published, and `request(None)` returns
                `None` without sending a request to the broker. Such skipped
                values are excluded from the generated AsyncAPI Message schema
                as well, while `None` types nested inside the message data
                (e.g. `dict[str, int | None]`) are preserved.
            persistent: Whether to retain the publisher across broker restarts.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
            include_in_schema: Whether to include operation in AsyncAPI schema.
        """
        super().__init__(
            topic,
            qos=qos,
            retain=retain,
            headers=headers,
            skip_none=skip_none,
            persistent=persistent,
            title=title,
            description=description,
            schema=schema,
            include_in_schema=include_in_schema,
        )


class MQTTRoute(SubscriberRoute):
    """Class to store delayed MQTTBroker subscriber registration."""

    def __init__(
        self,
        call: Callable[..., "SendableMessage"]
        | Callable[..., Awaitable["SendableMessage"]],
        topic: str,
        *,
        publishers: Iterable["MQTTPublisher"] = (),
        qos: QoS = QoS.AT_MOST_ONCE,
        shared: str | None = None,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        max_workers: int = 1,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> None:
        super().__init__(
            call,
            topic,
            publishers=publishers,
            persistent=persistent,
            qos=qos,
            shared=shared,
            ack_policy=ack_policy,
            no_reply=no_reply,
            dependencies=dependencies,
            parser=parser,
            decoder=decoder,
            max_workers=max_workers,
            title=title,
            description=description,
            include_in_schema=include_in_schema,
        )


class MQTTRouter(
    MQTTRegistrator,
    BrokerRouter["Any"],
):
    """Includable to MQTTBroker router."""

    def __init__(
        self,
        prefix: str = "",
        handlers: Iterable[MQTTRoute] = (),
        *,
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        routers: Iterable[MQTTRegistrator] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        include_in_schema: bool | None = None,
        ack_policy: AckPolicy = EMPTY,
    ) -> None:
        super().__init__(
            handlers=handlers,
            config=MQTTBrokerConfig(
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
