from abc import abstractmethod
from collections.abc import AsyncIterator, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Optional,
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
    from faststream._internal.endpoint.publisher import BasePublisherProto
    from faststream.confluent.configs import KafkaBrokerConfig, KafkaSubscriberConfig
    from faststream.confluent.helpers.client import AsyncConfluentConsumer
    from faststream.message import StreamMessage


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    _outer_config: "KafkaBrokerConfig"

    group_id: Optional[str]

    consumer: Optional["AsyncConfluentConsumer"]
    parser: AsyncConfluentParser

    def __init__(self, config: "KafkaSubscriberConfig", /) -> None:
        super().__init__(config)

        self.__connection_data = config.connection_data

        self.group_id = config.group_id

        self._topics = config.topics
        self._partitions = config.partitions

        self.consumer = None
        self.polling_interval = config.polling_interval

    @property
    def client_id(self) -> Optional[str]:
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
            self.add_task(self._consume())

    async def close(self) -> None:
        await super().close()

        if self.consumer is not None:
            await self.consumer.stop()
            self.consumer = None

    @override
    async def get_one(
        self,
        *,
        timeout: float = 5.0,
    ) -> "Optional[StreamMessage[MsgType]]":
        assert self.consumer, "You should start subscriber at first."  # nosec B101
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        raw_message = await self.consumer.getone(timeout=timeout)

        context = self._outer_config.fd_config.context

        return await process_msg(
            msg=raw_message,  # type: ignore[arg-type]
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=self._parser,
            decoder=self._decoder,
        )

    @override
    async def __aiter__(self) -> AsyncIterator["StreamMessage[MsgType]"]:  # type: ignore[override]
        assert self.consumer, "You should start subscriber at first."  # nosec B101
        assert (  # nosec B101
            not self.calls
        ), "You can't use iterator if subscriber has registered handlers."

        timeout = 5.0
        while True:
            raw_message = await self.consumer.getone(timeout=timeout)

            if raw_message is None:
                continue

            context = self._outer_config.fd_config.context

            yield await process_msg(
                msg=raw_message,  # type: ignore[arg-type]
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence["BasePublisherProto"]:
        return (
            KafkaFakePublisher(
                self._outer_config.producer,
                topic=message.reply_to,
            ),
        )

    async def consume_one(self, msg: MsgType) -> None:
        await self.consume(msg)

    @abstractmethod
    async def get_msg(self) -> Optional[MsgType]:
        raise NotImplementedError

    async def _consume(self) -> None:
        assert self.consumer, "You should start subscriber at first."  # nosec B101

        connected = True
        while self.running:
            try:
                msg = await self.get_msg()
            except KafkaException:  # pragma: no cover  # noqa: PERF203
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
        if self.topics:
            return list(self.topics)
        return [f"{p.topic}-{p.partition}" for p in self.partitions]

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[Any]"],
        topic: str,
        group_id: Optional[str] = None,
    ) -> dict[str, str]:
        return {
            "topic": topic,
            "group_id": group_id or "",
            "message_id": getattr(message, "message_id", ""),
        }


class DefaultSubscriber(LogicSubscriber[Message]):
    def __init__(self, config: "KafkaSubscriberConfig", /) -> None:
        self.parser = AsyncConfluentParser(is_manual=not config.ack_first)
        config.decoder = self.parser.decode_message
        config.parser = self.parser.parse_message
        super().__init__(config)

    async def get_msg(self) -> Optional["Message"]:
        assert self.consumer, "You should setup subscriber at first."  # nosec B101
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
        /,
        max_records: Optional[int],
    ) -> None:
        self.max_records = max_records

        self.parser = AsyncConfluentParser(is_manual=not config.ack_first)
        config.decoder = self.parser.decode_message_batch
        config.parser = self.parser.parse_message_batch
        super().__init__(config)

    async def get_msg(self) -> Optional[tuple["Message", ...]]:
        assert self.consumer, "You should setup subscriber at first."  # nosec B101
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
