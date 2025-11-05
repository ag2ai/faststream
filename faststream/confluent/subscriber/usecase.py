import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
    cast,
)

import anyio
from confluent_kafka import KafkaException, Message
from typing_extensions import override

from faststream._internal.endpoint.subscriber import SubscriberUsecase
from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import MsgType
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


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    _outer_config: "KafkaBrokerConfig"

    group_id: str | None

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

        self.group_id = config.group_id

        self._topics = config.topics
        self._partitions = config.partitions

        self.consumer = None
        self.polling_interval = config.polling_interval

    @property
    def client_id(self) -> str | None:
        return self._outer_config.client_id

    @property
    def topics(self) -> list[str]:
        return [f"{self._outer_config.prefix}{t}" for t in self._topics]

    @property
    def partitions(self) -> list[TopicPartition]:
        return [p.add_prefix(self._outer_config.prefix) for p in self._partitions]

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
        topics = self.topics or (f"{p.topic}-{p.partition}" for p in self.partitions)
        return [f"{self._outer_config.prefix}{t}" for t in topics]

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
    def __init__(
        self,
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[Message]",
    ) -> None:
        self.parser = AsyncConfluentParser(is_manual=not config.ack_first)
        config.decoder = self.parser.decode_message
        config.parser = self.parser.parse_message
        super().__init__(config, specification, calls)

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
        self.parser = AsyncConfluentParser(is_manual=not config.ack_first)
        config.decoder = self.parser.decode_message_batch
        config.parser = self.parser.parse_message_batch
        super().__init__(config, specification, calls)

        self.max_records = max_records

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
