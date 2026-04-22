from collections.abc import Iterable, Sequence
from typing import TYPE_CHECKING, Any, Optional, cast

from typing_extensions import override
from zmqtt import QoS

from faststream._internal.broker.registrator import Registrator
from faststream._internal.constants import EMPTY
from faststream.middlewares import AckPolicy
from faststream.mqtt.broker.config import MQTTBrokerConfig
from faststream.mqtt.publisher.factory import create_publisher
from faststream.mqtt.subscriber.factory import create_subscriber

if TYPE_CHECKING:
    import zmqtt  # noqa: F401
    from fast_depends.dependencies import Dependant

    from faststream._internal.parser import CodecProto
    from faststream._internal.types import (
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.mqtt.publisher.usecase import MQTTPublisher
    from faststream.mqtt.subscriber.usecase import (
        MQTTConcurrentSubscriber,
        MQTTDefaultSubscriber,
    )


class MQTTRegistrator(Registrator["zmqtt.Message", MQTTBrokerConfig]):
    """Includable to MQTTBroker router."""

    @override
    def subscriber(  # type: ignore[override]
        self,
        topic: str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        shared: str | None = None,
        # broker arguments
        ack_policy: AckPolicy = EMPTY,
        no_reply: bool = False,
        dependencies: Iterable["Dependant"] = (),
        parser: Optional["CustomCallable"] = None,
        decoder: Optional["CustomCallable"] = None,
        codec: Optional["CodecProto"] = None,
        max_workers: int = 1,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        include_in_schema: bool = True,
    ) -> "MQTTDefaultSubscriber | MQTTConcurrentSubscriber":
        """Subscribe a handler to an MQTT topic.

        Args:
            topic: MQTT topic filter. Wildcards ``+`` (single level) and
                ``#`` (multi-level) are supported.
            qos: QoS level for the subscription (0, 1, or 2).
             shared: Optional shared subscription group name. When set,
                 subscribes as ``$share/<group>/<topic>``.
             ack_policy: Acknowledgement policy for message processing.
             no_reply: Whether to disable FastStream RPC / reply-to responses.
             dependencies: Dependencies list to apply to the subscriber.
             parser: Custom parser to map raw messages to FastStream ones.
             decoder: Function to decode FastStream message bytes to Python objects.
             codec: Custom codec object.
             max_workers: Number of workers to process messages concurrently.
             persistent: Whether to retain the subscriber across broker restarts.
            title: AsyncAPI subscriber object title.
            description: AsyncAPI subscriber object description.
            include_in_schema: Whether to include operation in AsyncAPI schema.
        """
        subscriber = create_subscriber(
            topic=topic,
            qos=qos,
            shared=shared,
            ack_policy=ack_policy,
            no_reply=no_reply,
            config=cast("MQTTBrokerConfig", self.config),
            max_workers=max_workers,
            title_=title,
            description_=description,
            include_in_schema=include_in_schema,
        )

        super().subscriber(subscriber, persistent=persistent)

        return subscriber.add_call(
            parser_=parser or self._parser,
            decoder_=decoder or self._decoder,
            codec_=codec,
            dependencies_=dependencies,
        )

    @override
    def publisher(  # type: ignore[override]
        self,
        topic: str,
        *,
        qos: QoS = QoS.AT_MOST_ONCE,
        retain: bool = False,
        headers: dict[str, str] | None = None,
        persistent: bool = True,
        # AsyncAPI information
        title: str | None = None,
        description: str | None = None,
        schema: Any | None = None,
        include_in_schema: bool = True,
    ) -> "MQTTPublisher":
        """Create a persistent publisher object for the given MQTT topic.

        Args:
            topic: MQTT topic to publish to. Must not contain wildcards.
            qos: QoS level for published messages (0, 1, or 2).
            retain: Whether the broker should retain the last message.
            headers: Default headers to include in every published message.
            persistent: Whether to retain the publisher across broker restarts.
            title: AsyncAPI publisher object title.
            description: AsyncAPI publisher object description.
            schema: AsyncAPI publishing message type.
            include_in_schema: Whether to include operation in AsyncAPI schema.
        """
        publisher = create_publisher(
            topic=topic,
            qos=qos,
            retain=retain,
            headers=headers,
            broker_config=cast("MQTTBrokerConfig", self.config),
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
        router: "MQTTRegistrator",  # type: ignore[override]
        *,
        prefix: str = "",
        dependencies: Iterable["Dependant"] = (),
        middlewares: Sequence["BrokerMiddleware[Any, Any]"] = (),
        include_in_schema: bool | None = None,
    ) -> None:
        if not isinstance(router, MQTTRegistrator):
            msg = (
                f"Router must be an instance of MQTTRegistrator, "
                f"got {type(router).__name__} instead."
            )
            raise TypeError(msg)

        super().include_router(
            router,
            prefix=prefix,
            dependencies=dependencies,
            middlewares=middlewares,
            include_in_schema=include_in_schema,
        )

        for m in router.config.broker_middlewares:
            router.config._validate_middleware(m)
