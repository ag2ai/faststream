import asyncio
from collections.abc import Iterator
from contextlib import contextmanager
from typing import TYPE_CHECKING, Any, Literal, Optional, cast
from unittest.mock import MagicMock

import anyio
import zmqtt
from typing_extensions import override
from zmqtt._internal.protocol import _shared_filter_to_actual, _topic_matches

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.message import encode_message
from faststream.mqtt.broker.broker import MQTTBroker
from faststream.mqtt.parser import MQTTParserV5, MQTTParserV311
from faststream.mqtt.publisher.producer import ZmqttBaseProducer
from faststream.mqtt.response import MQTTPublishCommand

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream.mqtt.publisher.usecase import MQTTPublisher
    from faststream.mqtt.subscriber.usecase import MQTTBaseSubscriber

__all__ = ("TestMQTTBroker",)


class _BlockingSubscription:
    """Fake zmqtt.Subscription that blocks forever on iteration.

    Used by ``TestMQTTBroker`` so dynamic subscribers can call
    ``start()`` without a real MQTT connection.  Message routing
    happens through ``FakeProducer``, not through this iterator.
    """

    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def __aiter__(self) -> "_BlockingSubscription":
        return self

    async def __anext__(self) -> zmqtt.Message:
        # Block until the task is cancelled (i.e. subscriber.stop() is called)
        await asyncio.sleep(1e9)
        raise StopAsyncIteration  # pragma: no cover


def mqtt_topic_matches(pattern: str, topic: str) -> bool:
    return _topic_matches(_shared_filter_to_actual(pattern), topic)


def _broker_version(broker: MQTTBroker) -> Literal["3.1.1", "5.0"]:
    return getattr(broker.config.broker_config, "version", "5.0")


def _parser_for_version(
    version: Literal["3.1.1", "5.0"],
) -> MQTTParserV311 | MQTTParserV5:
    return MQTTParserV311() if version == "3.1.1" else MQTTParserV5()


class TestMQTTBroker(TestBroker[MQTTBroker]):
    """In-memory test double for MQTTBroker.

    Routes published messages to matching subscribers without a real
    MQTT connection, using MQTT wildcard rules for topic matching.
    Messages are encoded in the same wire format as the configured
    broker version (V311 envelope or V5 PublishProperties).

    Usage::

        async with TestMQTTBroker(broker) as br:
            await br.publish("hello", "sensors/temp")
            handler.mock.assert_called_once_with("hello")
    """

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: MQTTBroker,
        publisher: "MQTTPublisher",
    ) -> tuple["MQTTBaseSubscriber", bool]:
        sub: MQTTBaseSubscriber | None = None
        for handler in broker.subscribers:
            handler = cast("MQTTBaseSubscriber", handler)
            if mqtt_topic_matches(handler.topic, publisher.topic):
                sub = handler
                break

        if sub is None:
            is_real = False
            sub = broker.subscriber(publisher.topic, persistent=False)
            # Apply the correct version parser so fake subs match FakeProducer output.
            parser = _parser_for_version(_broker_version(broker))
            sub._parser = parser.parse_message
            sub._decoder = parser.decode_message
        else:
            is_real = True

        return sub, is_real

    def _fake_start(self, broker: MQTTBroker, *args: Any, **kwargs: Any) -> None:
        # Ensure all pre-existing subscribers use the version-correct parser
        # before patch_broker_calls builds the fastdepends model.
        parser = _parser_for_version(_broker_version(broker))
        for sub in cast("list[MQTTBaseSubscriber]", broker.subscribers):
            sub._parser = parser.parse_message
            sub._decoder = parser.decode_message
        super()._fake_start(broker, *args, **kwargs)

    @contextmanager
    def _patch_producer(self, broker: MQTTBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker)
        with change_producer(broker.config.broker_config, fake_producer):
            yield

    async def _fake_connect(  # type: ignore[override]
        self,
        broker: MQTTBroker,
        *args: Any,
        **kwargs: Any,
    ) -> MagicMock:
        fake_client = MagicMock()
        fake_client.subscribe.return_value = _BlockingSubscription()
        # Wire fake client into config so that dynamically-added subscribers
        # can call start() without a real MQTT connection.
        broker.config.broker_config._client = fake_client
        return fake_client


class FakeProducer(ZmqttBaseProducer):
    """In-memory producer that routes messages directly to matching subscribers.

    Encodes messages in the wire format matching the broker's configured
    MQTT version: V311 envelope for 3.1.1, PublishProperties for 5.0.
    """

    def __init__(self, broker: MQTTBroker) -> None:
        self.broker = broker
        self.serializer: SerializerProto | None = None

        version = _broker_version(broker)
        default = _parser_for_version(version)
        self._parser = ParserComposition(broker._parser, default.parse_message)
        self._decoder = ParserComposition(broker._decoder, default.decode_message)

    @property
    def _version(self) -> Literal["3.1.1", "5.0"]:
        return _broker_version(self.broker)

    @override
    async def publish(self, cmd: MQTTPublishCommand) -> None:
        msg = build_message(
            message=cmd.body,
            topic=cmd.destination,
            version=self._version,
            qos=cmd.qos,
            retain=cmd.retain,
            reply_to=cmd.reply_to,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            serializer=self.broker.config.fd_config._serializer,
        )

        # For shared subscriptions, only deliver to one subscriber per group
        seen_shared_groups: set[str] = set()

        for handler in cast("list[MQTTBaseSubscriber]", self.broker.subscribers):
            handler_topic = handler.topic
            if not mqtt_topic_matches(handler_topic, cmd.destination):
                continue

            if handler_topic.startswith("$share/"):
                _, group, _ = handler_topic.split("/", 2)
                if group in seen_shared_groups:
                    continue
                seen_shared_groups.add(group)

            await handler.process_message(msg)

    @override
    async def request(self, cmd: MQTTPublishCommand) -> "zmqtt.Message":
        msg = build_message(
            message=cmd.body,
            topic=cmd.destination,
            version=self._version,
            qos=cmd.qos,
            retain=cmd.retain,
            correlation_id=cmd.correlation_id,
            headers=cmd.headers,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in cast("list[MQTTBaseSubscriber]", self.broker.subscribers):
            if not mqtt_topic_matches(handler.topic, cmd.destination):
                continue

            with anyio.fail_after(cmd.timeout or 30.0):
                result = await handler.process_message(msg)

            return build_message(
                message=result.body,
                topic=cmd.destination,
                version=self._version,
                correlation_id=result.correlation_id,
                headers=result.headers,
                serializer=self.broker.config.fd_config._serializer,
            )

        raise SubscriberNotFound


def build_message(
    message: "SendableMessage",
    topic: str,
    *,
    version: Literal["3.1.1", "5.0"] = "5.0",
    qos: int = 0,
    retain: bool = False,
    reply_to: str = "",
    correlation_id: str | None = None,
    headers: dict[str, str] | None = None,
    serializer: Optional["SerializerProto"] = None,
) -> zmqtt.Message:
    """Build a fake ``zmqtt.Message`` from publish parameters.

    For MQTT 5.0 uses *PublishProperties* to carry metadata so that
    ``MQTTParserV5`` can extract them transparently.
    For MQTT 3.1.1 returns a plain message with raw payload only.
    """
    payload, content_type = encode_message(message, serializer=serializer)

    if version == "3.1.1":
        return zmqtt.Message(
            topic=topic,
            payload=payload,
            qos=zmqtt.QoS(qos),
            retain=retain,
        )

    user_props: list[tuple[str, str]] = list((headers or {}).items())

    properties = zmqtt.PublishProperties(
        content_type=content_type or None,
        response_topic=reply_to or None,
        correlation_data=correlation_id.encode() if correlation_id else None,
        user_properties=tuple(user_props),
    )

    return zmqtt.Message(
        topic=topic,
        payload=payload,
        qos=zmqtt.QoS(qos),
        retain=retain,
        properties=properties,
    )
