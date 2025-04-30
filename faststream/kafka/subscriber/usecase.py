from abc import abstractmethod
from collections.abc import Sequence
from itertools import chain
from typing import TYPE_CHECKING, Any, AsyncIterator, Callable, Optional, cast

import anyio
from aiokafka import ConsumerRecord, TopicPartition
from aiokafka.errors import ConsumerStoppedError, KafkaError
from typing_extensions import override
from faststream._internal.subscriber.configs import SpecificationSubscriberConfigs
from faststream._internal.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.subscriber.usecase import SubscriberUsecase
from faststream._internal.subscriber.utils import process_msg
from faststream._internal.types import (
    CustomCallable,
    MsgType,
)
from faststream._internal.utils.path import compile_path
from faststream.kafka.listener import make_logging_listener
from faststream.kafka.message import KafkaAckableMessage, KafkaMessage, KafkaRawMessage
from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from faststream.kafka.publisher.fake import KafkaFakePublisher
from faststream.kafka.subscriber.configs import KafkaSubscriberBaseConfigs
from faststream.middlewares.acknowledgement.conf import AckPolicy

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer

    from faststream._internal.basic_types import AnyDict
    from faststream._internal.publisher.proto import BasePublisherProto
    from faststream._internal.state import BrokerState, Pointer
    from faststream.message import StreamMessage


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    topics: Sequence[str]
    group_id: Optional[str]

    builder: Optional[Callable[..., "AIOKafkaConsumer"]]
    consumer: Optional["AIOKafkaConsumer"]

    client_id: Optional[str]
    batch: bool
    parser: AioKafkaParser

    def __init__(self, base_configs: KafkaSubscriberBaseConfigs) -> None:
        super().__init__(configs=base_configs)

        self.topics = base_configs.topics
        self.partitions = base_configs.partitions
        self.group_id = base_configs.group_id

        self._pattern = base_configs.pattern
        self._listener = base_configs.listener
        self._connection_args = base_configs.connection_args

        # Setup it later
        self.client_id = ""
        self.builder = None

        self.consumer = None

    @override
    def _setup(  # type: ignore[override]
        self,
        *,
        client_id: Optional[str],
        builder: Callable[..., "AIOKafkaConsumer"],
        # basic args
        extra_context: "AnyDict",
        # broker base_configs
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        state: "Pointer[BrokerState]",
    ) -> None:
        self.client_id = client_id
        self.builder = builder

        super()._setup(
            extra_context=extra_context,
            broker_parser=broker_parser,
            broker_decoder=broker_decoder,
            state=state,
        )

    async def start(self) -> None:
        """Start the consumer."""
        assert self.builder, "You should setup subscriber at first."  # nosec B101

        self.consumer = consumer = self.builder(
            group_id=self.group_id,
            client_id=self.client_id,
            **self._connection_args,
        )

        self.parser._setup(consumer)

        if self.topics or self._pattern:
            consumer.subscribe(
                topics=self.topics,
                pattern=self._pattern,
                listener=make_logging_listener(
                    consumer=consumer,
                    logger=self._state.get().logger_state.logger.logger,
                    log_extra=self.get_log_context(None),
                    listener=self._listener,
                ),
            )

        elif self.partitions:
            consumer.assign(partitions=self.partitions)

        await consumer.start()
        await super().start()

        if self.calls:
            self.add_task(self._run_consume_loop(self.consumer))

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
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        assert self.consumer, "You should start subscriber at first."  # nosec B101

        raw_messages = await self.consumer.getmany(
            timeout_ms=timeout * 1000,
            max_records=1,
        )

        if not raw_messages:
            return None

        ((raw_message,),) = raw_messages.values()

        context = self._state.get().di_state.context

        return await process_msg(
            msg=raw_message,
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
        ), "You can't use `get_one` method if subscriber has registered handlers."

        async for raw_message in self.consumer:
            context = self._state.get().di_state.context
            msg: StreamMessage[MsgType] = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=self._parser,
                decoder=self._decoder,
            )
            yield msg

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

    @abstractmethod
    async def get_msg(self, consumer: "AIOKafkaConsumer") -> MsgType:
        raise NotImplementedError

    async def _run_consume_loop(self, consumer: "AIOKafkaConsumer") -> None:
        assert consumer, "You should start subscriber at first."  # nosec B101

        connected = True
        while self.running:
            try:
                msg = await self.get_msg(consumer)

            # pragma: no cover
            except KafkaError:  # noqa: PERF203
                if connected:
                    connected = False
                await anyio.sleep(5)

            except ConsumerStoppedError:
                return

            else:
                if not connected:  # pragma: no cover
                    connected = True

                if msg:
                    await self.consume_one(msg)

    async def consume_one(self, msg: MsgType) -> None:
        await self.consume(msg)

    @property
    def topic_names(self) -> list[str]:
        if self._pattern:
            return [self._pattern]
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
            )
            for p in self.partitions
        ]


class DefaultSubscriber(LogicSubscriber["ConsumerRecord"]):
    def __init__(self, base_configs: KafkaSubscriberBaseConfigs) -> None:
        if base_configs.pattern:
            reg, pattern = compile_path(
                base_configs.pattern,
                replace_symbol=".*",
                patch_regex=lambda x: x.replace(r"\*", ".*"),
            )
            base_configs.pattern = pattern

        else:
            reg = None

        self.parser = AioKafkaParser(
            msg_class=KafkaMessage
            if base_configs.ack_policy is base_configs.ack_policy.ACK_FIRST
            else KafkaAckableMessage,
            regex=reg,
        )
        base_configs.default_parser = self.parser.parse_message
        base_configs.default_decoder = self.parser.decode_message
        super().__init__(base_configs=base_configs)

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "ConsumerRecord":
        assert consumer, "You should setup subscriber at first."  # nosec B101
        return await consumer.getone()

    def get_log_context(
        self,
        message: Optional["StreamMessage[ConsumerRecord]"],
    ) -> dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message.topic

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class BatchSubscriber(LogicSubscriber[tuple["ConsumerRecord", ...]]):
    def __init__(
        self,
        base_configs: KafkaSubscriberBaseConfigs,
        batch_timeout_ms: int,
        max_records: Optional[int],
    ) -> None:
        self.batch_timeout_ms = batch_timeout_ms
        self.max_records = max_records

        if base_configs.pattern:
            reg, pattern = compile_path(
                base_configs.pattern,
                replace_symbol=".*",
                patch_regex=lambda x: x.replace(r"\*", ".*"),
            )
            base_configs.pattern = pattern

        else:
            reg = None

        self.parser = AioKafkaBatchParser(
            msg_class=KafkaMessage
            if base_configs.ack_policy is AckPolicy.ACK_FIRST
            else KafkaAckableMessage,
            regex=reg,
        )
        base_configs.default_decoder = self.parser.decode_message
        base_configs.default_parser = self.parser.parse_message
        super().__init__(base_configs=base_configs)

    async def get_msg(
        self, consumer: "AIOKafkaConsumer"
    ) -> tuple["ConsumerRecord", ...]:
        assert consumer, "You should setup subscriber at first."  # nosec B101

        messages = await consumer.getmany(
            timeout_ms=self.batch_timeout_ms,
            max_records=self.max_records,
        )

        if not messages:  # pragma: no cover
            await anyio.sleep(self.batch_timeout_ms / 1000)
            return ()

        return tuple(chain(*messages.values()))

    def get_log_context(
        self,
        message: Optional["StreamMessage[tuple[ConsumerRecord, ...]]"],
    ) -> dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message[0].topic

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class ConcurrentDefaultSubscriber(ConcurrentMixin["ConsumerRecord"], DefaultSubscriber):
    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "ConsumerRecord") -> None:
        await self._put_msg(msg)


class ConcurrentBetweenPartitionsSubscriber(DefaultSubscriber):
    consumer_subgroup: list["AIOKafkaConsumer"]

    def __init__(
        self,
        base_configs: KafkaSubscriberBaseConfigs,
        max_workers: int
    ) -> None:
        super().__init__(
            base_configs=base_configs
        )

        self.max_workers = max_workers
        self.consumer_subgroup = []

    async def start(self) -> None:
        """Start the consumer subgroup."""
        assert self.builder, "You should setup subscriber at first."  # nosec B101

        if self.calls:
            self.consumer_subgroup = [
                self.builder(
                    group_id=self.group_id,
                    client_id=self.client_id,
                    **self._connection_args,
                )
                for _ in range(self.max_workers)
            ]

        else:
            # We should create single consumer to support
            # `get_one()` and `__aiter__` methods
            self.consumer = self.builder(
                group_id=self.group_id,
                client_id=self.client_id,
                **self._connection_args,
            )
            self.consumer_subgroup = [self.consumer]

        # Subscribers starting should be called concurrently
        # to balance them correctly
        async with anyio.create_task_group() as tg:
            for c in self.consumer_subgroup:
                c.subscribe(
                    topics=self.topics,
                    listener=make_logging_listener(
                        consumer=c,
                        logger=self._state.get().logger_state.logger.logger,
                        log_extra=self.get_log_context(None),
                        listener=self._listener,
                    ),
                )

                tg.start_soon(c.start)

        # call SubscriberUsecase method
        await super(LogicSubscriber, self).start()

        if self.calls:
            for c in self.consumer_subgroup:
                self.add_task(self._run_consume_loop(c))

    async def close(self) -> None:
        if self.consumer_subgroup:
            async with anyio.create_task_group() as tg:
                for consumer in self.consumer_subgroup:
                    tg.start_soon(consumer.stop)

            self.consumer_subgroup = []

        await super().close()

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "KafkaRawMessage":
        assert consumer, "You should setup subscriber at first."  # nosec B101
        message = await consumer.getone()
        message.consumer = consumer
        return cast("KafkaRawMessage", message)
