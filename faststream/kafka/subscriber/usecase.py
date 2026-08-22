import logging
from abc import abstractmethod
from collections.abc import AsyncIterator, Callable, Sequence
from itertools import chain
from typing import TYPE_CHECKING, Any, NamedTuple, Optional, cast

import anyio
from aiokafka import ConsumerRecord, TopicPartition
from aiokafka.errors import ConsumerStoppedError, KafkaError, UnsupportedCodecError
from typing_extensions import override

from faststream._internal.endpoint.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream._internal.endpoint.subscriber.usecase import SubscriberUsecase
from faststream._internal.endpoint.utils import process_msg
from faststream._internal.types import MsgType
from faststream._internal.utils.path import compile_path
from faststream.kafka.helpers import make_logging_listener
from faststream.kafka.message import KafkaAckableMessage, KafkaMessage, KafkaRawMessage
from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from faststream.kafka.publisher.fake import KafkaFakePublisher

if TYPE_CHECKING:
    from re import Pattern

    from aiokafka import AIOKafkaConsumer

    from faststream._internal.endpoint.publisher import PublisherProto
    from faststream._internal.endpoint.subscriber import SubscriberSpecification
    from faststream._internal.endpoint.subscriber.call_item import CallsCollection
    from faststream.kafka.configs import KafkaBrokerConfig
    from faststream.message import StreamMessage

    from .config import KafkaSubscriberConfig


class CompiledPattern(NamedTuple):
    """A Kafka pattern, and the regex extracting its path parameters."""

    regex: Optional["Pattern[str]"]
    pattern: str


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    consumer: Optional["AIOKafkaConsumer"]

    batch: bool
    parser: AioKafkaParser

    _outer_config: "KafkaBrokerConfig"

    def __init__(
        self,
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[MsgType]",
    ) -> None:
        super().__init__(config, specification, calls)

        self._topics = config.topics
        self._partitions = config.partitions
        self._group_id = config.group_id

        self._pattern = config.pattern
        self._listener = config.listener
        self._connection_args = config.connection_args

        self._compiled_pattern: CompiledPattern | None = None
        self._compiled_from: str | None = None

        self.consumer = None

    @property
    def group_id(self) -> str | None:
        return self._outer_config.resolve_option(self._group_id)

    @property
    def pattern(self) -> str | None:
        if not self._pattern:
            return None
        return self._compile_pattern().pattern

    def get_pattern_regex(self) -> Optional["Pattern[str]"]:
        """The regex pulling path parameters out of a topic name, if any.

        A method rather than a property because the parser holds on to it and
        asks per message, the pattern being resolved anew on every read.
        """
        if not self._pattern:
            return None
        return self._compile_pattern().regex

    def _compile_pattern(self) -> CompiledPattern:
        """Compile the pattern this subscriber resolves to right now.

        Kafka patterns are plain strings, so — unlike the brokers whose addresses
        are value objects — there is no constructor to compile the path inside
        after resolution. Memoised on the resolved value, which only changes when
        the Config values in scope do.
        """
        assert self._pattern is not None

        pattern = self._outer_config.resolve_address(self._pattern)

        if self._compiled_from != pattern:
            regex, compiled = compile_path(
                pattern,
                replace_symbol=".*",
                patch_regex=lambda x: x.replace(r"\*", ".*"),
            )
            self._compiled_from = pattern
            self._compiled_pattern = CompiledPattern(regex=regex, pattern=compiled)

        assert self._compiled_pattern is not None
        return self._compiled_pattern

    @property
    def topics(self) -> list[str]:
        return [self._outer_config.resolve_address(t) for t in self._topics]

    @property
    def partitions(self) -> list[TopicPartition]:
        return [
            TopicPartition(
                topic=self._outer_config.resolve_address(p.topic),
                partition=p.partition,
            )
            for p in self._partitions
        ]

    @property
    def builder(self) -> Callable[..., "AIOKafkaConsumer"]:
        return self._outer_config.builder

    @property
    def client_id(self) -> str | None:
        return self._outer_config.client_id

    async def start(self) -> None:
        """Start the consumer."""
        await super().start()

        self.consumer = consumer = self.builder(
            group_id=self.group_id,
            client_id=self.client_id,
            **self._connection_args,
        )

        self.parser._setup(consumer)

        if self.topics or self.pattern:
            consumer.subscribe(
                topics=self.topics,
                pattern=self.pattern,
                listener=make_logging_listener(
                    consumer=consumer,
                    logger=self._outer_config.logger.logger.logger,
                    log_extra=self.get_log_context(None),
                    listener=self._listener,
                ),
            )

        elif self.partitions:
            consumer.assign(partitions=self.partitions)

        await consumer.start()

        self._post_start()

        if self.calls:
            self.add_task(self._run_consume_loop, (self.consumer,))

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
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        assert self.consumer, "You should start subscriber at first."

        raw_messages = await self.consumer.getmany(
            timeout_ms=timeout * 1000,
            max_records=1,
        )

        if not raw_messages:
            return None

        ((raw_message,),) = raw_messages.values()

        context = self._outer_config.fd_config.context

        async_parser, async_decoder = self._get_parser_and_decoder()

        msg: KafkaMessage | None = await process_msg(  # type: ignore[assignment]
            msg=raw_message,
            middlewares=(
                m(raw_message, context=context) for m in self._broker_middlewares
            ),
            parser=async_parser,
            decoder=async_decoder,
        )
        return msg

    @override
    async def __aiter__(self) -> AsyncIterator["KafkaMessage"]:  # type: ignore[override]
        assert self.consumer, "You should start subscriber at first."
        assert not self.calls, (
            "You can't use `get_one` method if subscriber has registered handlers."
        )

        context = self._outer_config.fd_config.context
        async_parser, async_decoder = self._get_parser_and_decoder()

        async for raw_message in self.consumer:
            msg: KafkaMessage = await process_msg(  # type: ignore[assignment]
                msg=raw_message,
                middlewares=(
                    m(raw_message, context=context) for m in self._broker_middlewares
                ),
                parser=async_parser,
                decoder=async_decoder,
            )
            yield msg

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

    @abstractmethod
    async def get_msg(self, consumer: "AIOKafkaConsumer") -> MsgType:
        raise NotImplementedError

    async def _run_consume_loop(self, consumer: "AIOKafkaConsumer") -> None:
        assert consumer, "You should start subscriber at first."

        connected = True
        while self.running:
            try:
                msg = await self.get_msg(consumer)

            except UnsupportedCodecError as e:  # noqa: PERF203
                self._log(
                    logging.ERROR,
                    "There is no suitable compression library available. Please refer to the Kafka "
                    "documentation for more information - "
                    "https://aiokafka.readthedocs.io/en/stable/#installation",
                    exc_info=e,
                )
                await anyio.sleep(15)

            except KafkaError as e:
                self._log(logging.ERROR, "Kafka error occurred", exc_info=e)

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
        if self.pattern:
            topics = [self.pattern]

        elif self.topics:
            topics = self.topics

        else:
            topics = [f"{p.topic}-{p.partition}" for p in self.partitions]

        return topics

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


class DefaultSubscriber(LogicSubscriber["ConsumerRecord"]):
    def __init__(
        self,
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[ConsumerRecord]",
    ) -> None:
        self.parser = AioKafkaParser(
            msg_class=KafkaMessage if config.ack_first else KafkaAckableMessage,
            regex=self.get_pattern_regex,
        )
        config.parser = self.parser.parse_message
        config.decoder = self.parser.decode_message
        super().__init__(config, specification, calls)

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "ConsumerRecord":
        assert consumer, "You should setup subscriber at first."
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
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[tuple[ConsumerRecord, ...]]",
        batch_timeout_ms: int,
        max_records: int | None,
    ) -> None:
        self.parser = AioKafkaBatchParser(
            msg_class=KafkaMessage if config.ack_first else KafkaAckableMessage,
            regex=self.get_pattern_regex,
        )
        config.decoder = self.parser.decode_batch
        config.parser = self.parser.parse_batch
        super().__init__(config, specification, calls)

        self.batch_timeout_ms = batch_timeout_ms
        self.max_records = max_records

    async def get_msg(
        self,
        consumer: "AIOKafkaConsumer",
    ) -> tuple["ConsumerRecord", ...]:
        assert consumer, "You should setup subscriber at first."

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
        config: "KafkaSubscriberConfig",
        specification: "SubscriberSpecification[Any, Any]",
        calls: "CallsCollection[ConsumerRecord]",
        max_workers: int,
    ) -> None:
        super().__init__(config, specification, calls)

        self.max_workers = max_workers
        self.consumer_subgroup = []

    async def start(self) -> None:
        """Start the consumer subgroup."""
        await super(LogicSubscriber, self).start()

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
                        logger=self._outer_config.logger.logger.logger,
                        log_extra=self.get_log_context(None),
                        listener=self._listener,
                    ),
                )

                tg.start_soon(c.start)

        self._post_start()

        if self.calls:
            for c in self.consumer_subgroup:
                self.add_task(self._run_consume_loop, (c,))

    async def stop(self) -> None:
        if self.consumer_subgroup:
            async with anyio.create_task_group() as tg:
                for consumer in self.consumer_subgroup:
                    tg.start_soon(consumer.stop)

            self.consumer_subgroup = []

        await super().stop()

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "KafkaRawMessage":
        assert consumer, "You should setup subscriber at first."
        message = await consumer.getone()
        message.consumer = consumer
        return cast("KafkaRawMessage", message)
