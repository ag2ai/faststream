from abc import abstractmethod
from collections.abc import AsyncIterator, Iterable, Sequence
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)

import anyio
from confluent_kafka import KafkaException, Message
from typing_extensions import override

from faststream._internal.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.subscriber.usecase import SubscriberUsecase
from faststream._internal.subscriber.utils import process_msg
from faststream._internal.types import MsgType
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.publisher.fake import KafkaFakePublisher
from faststream.confluent.schemas import TopicPartition
from faststream.middlewares import AckPolicy

if TYPE_CHECKING:
    from fast_depends.dependencies import Dependant

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.publisher.proto import BasePublisherProto
    from faststream._internal.state import BrokerState
    from faststream._internal.types import (
        AsyncCallable,
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.confluent.client import AsyncConfluentConsumer
    from faststream.message import StreamMessage


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    topics: Sequence[str]
    group_id: Optional[str]

    builder: Optional[Callable[..., "AsyncConfluentConsumer"]]
    consumer: Optional["AsyncConfluentConsumer"]
    parser: AsyncConfluentParser

    client_id: Optional[str]

    def __init__(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        # Kafka information
        group_id: Optional[str],
        connection_data: "AnyDict",
        # Subscriber args
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
    ) -> None:
        super().__init__(
            default_parser=default_parser,
            default_decoder=default_decoder,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

        self.__connection_data = connection_data

        self.group_id = group_id
        self.topics = topics
        self.partitions = partitions

        self.consumer = None
        self.polling_interval = polling_interval

        # Setup it later
        self.client_id = ""
        self.builder = None

    @override
    def _setup(  # type: ignore[override]
        self,
        *,
        client_id: Optional[str],
        builder: Callable[..., "AsyncConfluentConsumer"],
        # basic args,
        extra_context: "AnyDict",
        # broker options
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        state: "BrokerState",
    ) -> None:
        self.client_id = client_id
        self.builder = builder

        super()._setup(
            extra_context=extra_context,
            broker_parser=broker_parser,
            broker_decoder=broker_decoder,
            state=state,
        )

    @override
    async def start(self) -> None:
        """Start the consumer."""
        assert self.builder, "You should setup subscriber at first."  # nosec B101

        self.consumer = consumer = self.builder(
            *self.topics,
            partitions=self.partitions,
            group_id=self.group_id,
            client_id=self.client_id,
            **self.__connection_data,
        )
        self.parser._setup(consumer)
        await consumer.start()

        await super().start()

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

        context = self._state.get().di_state.context

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

            context = self._state.get().di_state.context

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
                self._state.get().producer,
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

    def add_prefix(self, prefix: str) -> None:
        self.topics = tuple(f"{prefix}{t}" for t in self.topics)

        self.partitions = [
            TopicPartition(
                topic=f"{prefix}{p.topic}",
                partition=p.partition,
                offset=p.offset,
                metadata=p.metadata,
                leader_epoch=p.leader_epoch,
            )
            for p in self.partitions
        ]


class DefaultSubscriber(LogicSubscriber[Message]):
    def __init__(
        self,
        *topics: str,
        # Kafka information
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        group_id: Optional[str],
        connection_data: "AnyDict",
        # Subscriber args
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Sequence["BrokerMiddleware[Message]"],
    ) -> None:
        self.parser = AsyncConfluentParser(
            is_manual=ack_policy is not AckPolicy.ACK_FIRST
        )

        super().__init__(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            connection_data=connection_data,
            # subscriber args
            default_parser=self.parser.parse_message,
            default_decoder=self.parser.decode_message,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

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
        *topics: str,
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        max_records: Optional[int],
        # Kafka information
        group_id: Optional[str],
        connection_data: "AnyDict",
        # Subscriber args
        ack_policy: "AckPolicy",
        no_reply: bool,
        broker_dependencies: Iterable["Dependant"],
        broker_middlewares: Sequence["BrokerMiddleware[tuple[Message, ...]]"],
    ) -> None:
        self.max_records = max_records

        self.parser = AsyncConfluentParser(
            is_manual=ack_policy is not AckPolicy.ACK_FIRST
        )

        super().__init__(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            connection_data=connection_data,
            # subscriber args
            default_parser=self.parser.parse_message_batch,
            default_decoder=self.parser.decode_message_batch,
            # Propagated args
            ack_policy=ack_policy,
            no_reply=no_reply,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
        )

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
