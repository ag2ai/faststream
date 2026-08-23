import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    NamedTuple,
    Optional,
    cast,
)

import anyio
from confluent_kafka import KafkaException, Message
from typing_extensions import override

from faststream._internal.endpoint.derived import Resolved
from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import MsgType
from faststream._internal.utils.path import Address
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.publisher.fake import KafkaFakePublisher
from faststream.confluent.schemas import TopicPartition

if TYPE_CHECKING:
    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.confluent.configs import KafkaBrokerConfig
    from faststream.confluent.helpers.client import AsyncConfluentConsumer
    from faststream.confluent.message import KafkaMessage
    from faststream.message import StreamMessage

    from .config import KafkaSubscriberConfig


class _ResolvedAddresses(NamedTuple):
    """What a Confluent Subscriber listens on, once its composition is final.

    Resolved together and written once, so that the three reads below cannot
    disagree with each other or with what the consumer subscribed to.
    """

    topics: list[str]
    partitions: list[TopicPartition]
    group_id: str | None


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    _outer_config: "KafkaBrokerConfig"

    consumer: Optional["AsyncConfluentConsumer"]
    parser: AsyncConfluentParser

    def __init__(
        self,
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[MsgType]",
    ) -> None:
        super().__init__(config, specification, calls)

        self.__connection_data = config.connection_data

        self._group_id = config.group_id

        self._topics = config.topics
        self._partitions = config.partitions
        self._is_manual = not config.ack_first

        self._resolved: Resolved[_ResolvedAddresses] = self._derived.add(
            Resolved("a Subscriber's addresses"),
        )

        self.consumer = None
        self.polling_interval = config.polling_interval

    @override
    def _prepare(self) -> None:
        """Resolve what this Subscriber listens on, before anything reads it.

        First, because everything performed afterwards — the address check, the
        log context the logger is built from — reads these values back as fields.
        """
        config = self._outer_config

        self._resolved.set(
            _ResolvedAddresses(
                topics=[config.resolve_address(t) for t in self._topics],
                # `add_prefix` rather than the rebuild Kafka performs: a
                # partition carries an offset and a leader epoch beside its
                # topic, and only the topic is an address. That topic is typed
                # `str` here, so the prefix is all resolution has to reach it
                # until Confluent takes placeholders a level in (ADR-0006).
                partitions=[p.add_prefix(config.prefix) for p in self._partitions],
                group_id=config.resolve_option(self._group_id),
            ),
        )

        super()._prepare()

    @property
    def client_id(self) -> str | None:
        return self._outer_config.client_id

    @property
    def group_id(self) -> str | None:
        return self._resolved.get().group_id

    @property
    def topics(self) -> list[str]:
        return self._resolved.get().topics

    @property
    def partitions(self) -> list[TopicPartition]:
        return self._resolved.get().partitions

    @override
    def subscription_addresses(self) -> Iterable[Address]:
        """Every topic this Subscriber listens on, and none of them a template.

        Confluent subscribes by topic name only — there is no pattern
        subscription — and its parser never matches a message against a capture
        regex. So a `{param}` in an address is a character like any other, and
        `Address.literal` is what says so: a `Path()` parameter naming one is
        refused at Preparation instead of going unfilled for every message.
        """
        config = self._outer_config

        # The declared option travels alongside the resolved topic so that a
        # failure can name the Config value the topic came from.
        for declared, topic in zip(self._topics, self.topics, strict=True):
            yield Address.literal(topic, config.config_key(declared))

        # A partition names a topic too, and that name never holds a placeholder:
        # `partitions` takes structures rather than addresses.
        for partition in self.partitions:
            yield Address.literal(partition.topic)

    @override
    async def start(self) -> None:
        """Start the consumer."""
        await super().start()
        self.consumer = consumer = self._outer_config.builder(
            *self.topics,
            partitions=self.partitions,
            group_id=self.group_id,
            client_id=self.client_id,
            **self.__connection_data,
        )
        self.parser._setup(consumer)
        await consumer.start()

        self._post_start()

        if self.calls:
            self.add_task(self._consume)

    async def stop(self) -> None:
        await super().stop()

        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "KafkaMessage | None":
        assert self.consumer, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        raw_message = await self.consumer.getone(timeout=timeout)

        context = self._outer_config.fd_config.context

        async_parser, async_decoder = self._get_parser_and_decoder()

        return await process_msg(  # type: ignore[return-value]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )

    @override
    async def __aiter__(self) -> AsyncIterator["KafkaMessage"]:  # type: ignore[override]
        assert self.consumer, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use iterator if subscriber has registered handlers."
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        timeout = 5.0
        while True:
            raw_message = await self.consumer.getone(timeout=timeout)

            if raw_message is None:
                continue

            yield cast(
                "KafkaMessage",
                await process_msg(
                    msg=raw_message,
                    middlewares=(
                        m(raw_message, context=context) for m in self._broker_middlewares
                    ),
                    parser=async_parser,
                    decoder=async_decoder,
                ),
            )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["PublisherProto"]:
        return (
            KafkaFakePublisher(
                self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

    async def consume_one(self, msg: MsgType) -> None:
        await self.consume(msg)

    @abstractmethod
    async def get_msg(self) -> MsgType | None:
        raise NotImplementedError

    async def _consume(self) -> None:
        assert self.consumer, "You should start subscriber at first."

        connected = True
        while self.running:
            try:
                msg = await self.get_msg()

            except KafkaException as e:  # pragma: no cover  # noqa: PERF203
                self._log(
                    logging.ERROR,
                    message="Message fetch error",
                    exc_info=e,
                )

                if connected:
                    connected = False

                await anyio.sleep(5)

            else:
                if not connected:  # pragma: no cover
                    connected = True

                if msg is not None:
                    await self.consume_one(msg)

    @property
    def topic_names(self) -> list[str]:
        """The addresses a log line names, as this Subscriber answers them.

        `topics` and `partitions` are what Preparation resolved — the Router
        prefix composed, any Config value fixed — so a second pass over them
        would show an operator a doubled prefix and an address that does not
        exist.
        """
        if self.topics:
            return self.topics

        return [f"{p.topic}-{p.partition}" for p in self.partitions]

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[Any]"],
        topic: str,
        group_id: str | None = None,
    ) -> dict[str, str]:
        return {
            "topic": topic,
            "group_id": group_id or "",
            "message_id": getattr(message, "message_id", ""),
        }


class DefaultSubscriber(LogicSubscriber[Message]):
    @override
    def _build_parser(self) -> None:
        self.parser = AsyncConfluentParser(is_manual=self._is_manual)
        self._parser = self.parser.parse_message
        self._decoder = self.parser.decode_message

    async def get_msg(self) -> Optional["Message"]:
        assert self.consumer, "You should setup subscriber at first."
        return await self.consumer.getone(timeout=self.polling_interval)

    def get_log_context(
        self,
        message: Optional["StreamMessage[Message]"],
    ) -> dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message.topic() or ",".join(self.topics)

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class ConcurrentDefaultSubscriber(ConcurrentMixin["Message"], DefaultSubscriber):
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "Message") -> None:
        await self._put_msg(msg)


class BatchSubscriber(LogicSubscriber[tuple[Message, ...]]):
    def __init__(
        self,
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[tuple[Message, ...]]",
        max_records: int | None,
    ) -> None:
        super().__init__(config, specification, calls)

        self.max_records = max_records

    @override
    def _build_parser(self) -> None:
        self.parser = AsyncConfluentParser(is_manual=self._is_manual)
        self._parser = self.parser.parse_batch
        self._decoder = self.parser.decode_batch

    async def get_msg(self) -> tuple["Message", ...] | None:
        assert self.consumer, "You should setup subscriber at first."
        return (
            await self.consumer.getmany(
                timeout=self.polling_interval,
                max_records=self.max_records,
            )
            or None
        )

    def get_log_context(
        self,
        message: Optional["StreamMessage[tuple[Message, ...]]"],
    ) -> dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message[0].topic() or ",".join(self.topic_names)

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )
