import warnings
from abc import abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from contextlib import suppress
from typing import TYPE_CHECKING, Any

import anyio
import zmqtt
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.utils.path import Address, PrefixedRead
from faststream.middlewares import AckPolicy
from faststream.mqtt.parser import MQTTBaseParser, MQTTParserV5, MQTTParserV311
from faststream.mqtt.path import MQTT_ADDRESS_SYNTAX
from faststream.mqtt.publisher.fake import MQTTFakePublisher

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.message import StreamMessage
    from faststream.mqtt.broker.config import MQTTBrokerConfig
    from faststream.mqtt.message import MQTTMessage
    from faststream.mqtt.subscriber.config import MQTTSubscriberConfig


class MQTTBaseSubscriber(TasksMixin, SubscriberUsecase[zmqtt.Message]):
    """Base class for all MQTT subscribers."""

    _outer_config: "MQTTBrokerConfig"

    def __init__(
        self,
        config: "MQTTSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[zmqtt.Message]",
    ) -> None:
        # version may not be available yet when subscriber is created on a router
        # before include_router is called; default to V5 and re-resolve in
        # Preparation, once the composition reaches the Broker's own options.
        parser = self._make_parser(config._outer_config)
        config.parser = parser.parse_message
        config.decoder = parser.decode_message
        super().__init__(config, specification, calls)
        self._topic = config.topic
        self._address: PrefixedRead[Address] = self._derived.add(PrefixedRead())
        self._shared = config.shared
        self._qos = config.qos
        self._subscription: zmqtt.Subscription | None = None

        if config.ack_policy is AckPolicy.NACK_ON_ERROR:
            warnings.warn(
                "MQTT has no nack primitive; with NACK_ON_ERROR, "
                "on error QoS 1/2 messages will not be acknowledged "
                "and the broker will redeliver them.",
                RuntimeWarning,
                stacklevel=3,
            )

    @override
    def _build_parser(self) -> None:
        parser = self._make_parser(self._outer_config)
        self._parser = parser.parse_message
        self._decoder = parser.decode_message

    def _make_parser(self, outer_config: Any) -> MQTTBaseParser:
        version = getattr(outer_config, "version", "5.0")
        cls: type[MQTTBaseParser] = MQTTParserV311 if version == "3.1.1" else MQTTParserV5
        return cls(regex=lambda: self.address.regex)

    @property
    def address(self) -> "Address":
        """The topic this Subscriber was declared with, and its Broker address.

        Compiled here rather than at the declaration site, because that is the
        first moment the topic is known in full — the Router prefix composed,
        and any Config value resolved. Compiled once and kept: a
        Config value is fixed at `connect()` (ADR-0004).

        The Config key travels with the address, so a value that cannot compile
        can name the key to fix rather than only the string that arrived.
        """
        config = self._outer_config

        return self._address.read(
            config.prefix,
            lambda _: Address(
                config.resolve_address(self._topic),
                MQTT_ADDRESS_SYNTAX,
                config.config_key(self._topic),
            ),
        )

    @override
    def subscription_addresses(self) -> Iterable["Address"]:
        yield self.address

    @property
    def shared(self) -> str | None:
        """The shared-subscription group, resolved but never prefixed.

        `resolve_option` rather than `resolve_address`: a group name is not a
        topic, and a literal one has never been decorated with the Router prefix.
        """
        return self._outer_config.resolve_option(self._shared)

    @property
    def topic(self) -> str:
        """The topic MQTT is subscribed to, shared-subscription prefix included."""
        full = self.address.broker_address
        shared = self.shared
        return f"$share/{shared}/{full}" if shared else full

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        return (
            MQTTFakePublisher(
                producer=self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

    @staticmethod
    def build_log_context(
        message: "StreamMessage[zmqtt.Message] | None",
        topic: str = "",
    ) -> dict[str, str]:
        return {
            "topic": topic,
            "message_id": getattr(message, "message_id", ""),
        }

    def get_log_context(
        self,
        message: "StreamMessage[zmqtt.Message] | None",
    ) -> dict[str, str]:
        return self.build_log_context(message=message, topic=self.topic)

    @override
    async def start(self) -> None:
        await super().start()

        if self.calls:
            await self._create_subscription()
            self.add_task(self._consume_loop)

        self._post_start()

    @override
    async def stop(self) -> None:
        await super().stop()
        if self._subscription is not None:
            with suppress(Exception):
                await self._subscription.stop()
            self._subscription = None

    async def _create_subscription(self) -> None:
        auto_ack = self.ack_policy is AckPolicy.ACK_FIRST
        self._subscription = self._outer_config.client.subscribe(
            self.topic,
            qos=zmqtt.QoS(self._qos),
            auto_ack=auto_ack,
        )
        await self._subscription.start()

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "StreamMessage[zmqtt.Message] | None":
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        if self._subscription is None:
            auto_ack = self.ack_policy is AckPolicy.ACK_FIRST
            self._subscription = self._outer_config.client.subscribe(
                self.topic,
                qos=zmqtt.QoS(self._qos),
                auto_ack=auto_ack,
            )
            await self._subscription.start()

        async_parser, async_decoder = self._get_parser_and_decoder()

        raw_msg: zmqtt.Message | None = None
        with anyio.move_on_after(timeout):
            raw_msg = await self._subscription.get_message()

        context = self._outer_config.fd_config.context
        return await process_msg(
            msg=raw_msg,
            middlewares=(m(raw_msg, context=context) for m in self._broker_middlewares),
            parser=async_parser,
            decoder=async_decoder,
        )

    @override
    async def __aiter__(self) -> AsyncIterator["StreamMessage[zmqtt.Message]"]:  # type: ignore[override]
        if self._subscription is None:
            await self._create_subscription()

        assert self._subscription is not None
        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()
        async for raw_msg in self._subscription:
            msg: MQTTMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_msg,
                middlewares=(
                    m(raw_msg, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg

    @abstractmethod
    async def _consume_loop(self) -> None:
        raise NotImplementedError


class MQTTDefaultSubscriber(MQTTBaseSubscriber):
    """Sequential MQTT subscriber — processes one message at a time."""

    async def _consume_loop(self) -> None:
        assert self._subscription is not None
        async for msg in self._subscription:
            await self.consume(msg)


class MQTTConcurrentSubscriber(ConcurrentMixin[zmqtt.Message], MQTTBaseSubscriber):
    """Concurrent MQTT subscriber — processes up to max_workers messages in parallel."""

    @override
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def _consume_loop(self) -> None:
        assert self._subscription is not None
        async for msg in self._subscription:
            await self._put_msg(msg)
