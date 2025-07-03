import logging
from abc import ABC, abstractmethod
from itertools import chain
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Dict,
    Iterable,
    List,
    Optional,
    Sequence,
    Tuple,
    cast,
)

import anyio
from aiokafka import ConsumerRecord, TopicPartition
from aiokafka.errors import ConsumerStoppedError, KafkaError, UnsupportedCodecError
from typing_extensions import override

from faststream.broker.publisher.fake import FakePublisher
from faststream.broker.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream.broker.subscriber.usecase import SubscriberUsecase
from faststream.broker.types import (
    AsyncCallable,
    BrokerMiddleware,
    CustomCallable,
    MsgType,
)
from faststream.broker.utils import process_msg
from faststream.kafka.listener import LoggingListenerProxy
from faststream.kafka.message import KafkaAckableMessage, KafkaMessage, KafkaRawMessage
from faststream.kafka.parser import AioKafkaBatchParser, AioKafkaParser
from faststream.utils.path import compile_path

if TYPE_CHECKING:
    from aiokafka import AIOKafkaConsumer
    from aiokafka.abc import ConsumerRebalanceListener
    from fast_depends.dependencies import Depends

    from faststream.broker.message import StreamMessage
    from faststream.broker.publisher.proto import ProducerProto
    from faststream.types import AnyDict, Decorator, LoggerProto


class LogicSubscriber(ABC, TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    topics: Sequence[str]
    group_id: Optional[str]

    builder: Optional[Callable[..., "AIOKafkaConsumer"]]
    consumer: Optional["AIOKafkaConsumer"]

    client_id: Optional[str]
    batch: bool

    def __init__(
        self,
        *topics: str,
        # Kafka information
        group_id: Optional[str],
        connection_args: "AnyDict",
        listener: Optional["ConsumerRebalanceListener"],
        pattern: Optional[str],
        partitions: Iterable["TopicPartition"],
        # Subscriber args
        default_parser: "AsyncCallable",
        default_decoder: "AsyncCallable",
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[MsgType]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            default_parser=default_parser,
            default_decoder=default_decoder,
            # Propagated args
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

        self.topics = topics
        self.partitions = partitions
        self.group_id = group_id

        self._pattern = pattern
        self._listener = listener
        self._connection_args = connection_args

        # Setup it later
        self.client_id = ""
        self.builder = None

        self.consumer = None

    @override
    def setup(  # type: ignore[override]
        self,
        *,
        client_id: Optional[str],
        builder: Callable[..., "AIOKafkaConsumer"],
        # basic args
        logger: Optional["LoggerProto"],
        producer: Optional["ProducerProto"],
        graceful_timeout: Optional[float],
        extra_context: "AnyDict",
        # broker options
        broker_parser: Optional["CustomCallable"],
        broker_decoder: Optional["CustomCallable"],
        # dependant args
        apply_types: bool,
        is_validate: bool,
        _get_dependant: Optional[Callable[..., Any]],
        _call_decorators: Iterable["Decorator"],
    ) -> None:
        self.client_id = client_id
        self.builder = builder

        super().setup(
            logger=logger,
            producer=producer,
            graceful_timeout=graceful_timeout,
            extra_context=extra_context,
            broker_parser=broker_parser,
            broker_decoder=broker_decoder,
            apply_types=apply_types,
            is_validate=is_validate,
            _get_dependant=_get_dependant,
            _call_decorators=_call_decorators,
        )

    async def start(self) -> None:
        """Start the consumer."""
        assert self.builder, "You should setup subscriber at first."  # nosec B101

        self.consumer = consumer = self.builder(
            group_id=self.group_id,
            client_id=self.client_id,
            **self._connection_args,
        )

        if self.topics or self._pattern:
            consumer.subscribe(
                topics=self.topics,
                pattern=self._pattern,
                listener=LoggingListenerProxy(
                    consumer=consumer, logger=self.logger, listener=self._listener
                ),
            )

        elif self.partitions:
            consumer.assign(partitions=self.partitions)

        await consumer.start()
        await super().start()

        if self.calls:
            self.add_task(self._run_consume_loop(self.consumer))

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
    ) -> "Optional[StreamMessage[MsgType]]":
        assert self.consumer, "You should start subscriber at first."  # nosec B101
        assert (  # nosec B101
            not self.calls
        ), "You can't use `get_one` method if subscriber has registered handlers."

        raw_messages = await self.consumer.getmany(
            timeout_ms=timeout * 1000, max_records=1
        )

        if not raw_messages:
            return None

        ((raw_message,),) = raw_messages.values()

        return await process_msg(
            msg=raw_message,
            middlewares=self._broker_middlewares,
            parser=self._parser,
            decoder=self._decoder,
        )

    def _make_response_publisher(
        self,
        message: "StreamMessage[Any]",
    ) -> Sequence[FakePublisher]:
        if self._producer is None:
            return ()

        return (
            FakePublisher(
                self._producer.publish,
                publish_kwargs={
                    "topic": message.reply_to,
                },
            ),
        )

    @abstractmethod
    async def get_msg(self, consumer: "AIOKafkaConsumer") -> MsgType:
        raise NotImplementedError()

    async def _run_consume_loop(self, consumer: "AIOKafkaConsumer") -> None:
        assert consumer, "You should start subscriber at first."  # nosec B101

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

    @staticmethod
    def get_routing_hash(
        topics: Iterable[str],
        group_id: Optional[str] = None,
    ) -> int:
        return hash("".join((*topics, group_id or "")))

    @property
    def topic_names(self) -> List[str]:
        if self._pattern:
            return [self._pattern]
        elif self.topics:
            return list(self.topics)
        else:
            return [f"{p.topic}-{p.partition}" for p in self.partitions]

    def __hash__(self) -> int:
        return self.get_routing_hash(
            topics=self.topic_names,
            group_id=self.group_id,
        )

    @staticmethod
    def build_log_context(
        message: Optional["StreamMessage[Any]"],
        topic: str,
        group_id: Optional[str] = None,
    ) -> Dict[str, str]:
        return {
            "topic": topic,
            "group_id": group_id or "",
            "message_id": getattr(message, "message_id", ""),
        }

    def add_prefix(self, prefix: str) -> None:
        self.topics = tuple("".join((prefix, t)) for t in self.topics)

        self.partitions = [
            TopicPartition(
                topic="".join((prefix, p.topic)),
                partition=p.partition,
            )
            for p in self.partitions
        ]


class DefaultSubscriber(LogicSubscriber["ConsumerRecord"]):
    def __init__(
        self,
        *topics: str,
        # Kafka information
        group_id: Optional[str],
        listener: Optional["ConsumerRebalanceListener"],
        pattern: Optional[str],
        connection_args: "AnyDict",
        partitions: Iterable["TopicPartition"],
        is_manual: bool,
        # Subscriber args
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        if pattern:
            reg, pattern = compile_path(
                pattern,
                replace_symbol=".*",
                patch_regex=lambda x: x.replace(r"\*", ".*"),
            )

        else:
            reg = None

        parser = AioKafkaParser(
            msg_class=KafkaAckableMessage if is_manual else KafkaMessage,
            regex=reg,
        )

        super().__init__(
            *topics,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            # subscriber args
            default_parser=parser.parse_message,
            default_decoder=parser.decode_message,
            # Propagated args
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "ConsumerRecord":
        assert consumer, "You should setup subscriber at first."  # nosec B101
        return await consumer.getone()

    def get_log_context(
        self,
        message: Optional["StreamMessage[ConsumerRecord]"],
    ) -> Dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message.topic

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class BatchSubscriber(LogicSubscriber[Tuple["ConsumerRecord", ...]]):
    def __init__(
        self,
        *topics: str,
        batch_timeout_ms: int,
        max_records: Optional[int],
        # Kafka information
        group_id: Optional[str],
        listener: Optional["ConsumerRebalanceListener"],
        pattern: Optional[str],
        connection_args: "AnyDict",
        partitions: Iterable["TopicPartition"],
        is_manual: bool,
        # Subscriber args
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence[
            "BrokerMiddleware[Sequence[Tuple[ConsumerRecord, ...]]]"
        ],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        self.batch_timeout_ms = batch_timeout_ms
        self.max_records = max_records

        if pattern:
            reg, pattern = compile_path(
                pattern,
                replace_symbol=".*",
                patch_regex=lambda x: x.replace(r"\*", ".*"),
            )

        else:
            reg = None

        parser = AioKafkaBatchParser(
            msg_class=KafkaAckableMessage if is_manual else KafkaMessage,
            regex=reg,
        )

        super().__init__(
            *topics,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            # subscriber args
            default_parser=parser.parse_message,
            default_decoder=parser.decode_message,
            # Propagated args
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )

    async def get_msg(
        self, consumer: "AIOKafkaConsumer"
    ) -> Tuple["ConsumerRecord", ...]:
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
        message: Optional["StreamMessage[Tuple[ConsumerRecord, ...]]"],
    ) -> Dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message[0].topic

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class ConcurrentDefaultSubscriber(ConcurrentMixin[ConsumerRecord], DefaultSubscriber):
    def __init__(
        self,
        *topics: str,
        # Kafka information
        group_id: Optional[str],
        listener: Optional["ConsumerRebalanceListener"],
        pattern: Optional[str],
        connection_args: "AnyDict",
        partitions: Iterable["TopicPartition"],
        is_manual: bool,
        # Subscriber args
        max_workers: int,
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            *topics,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            is_manual=is_manual,
            # Propagated args
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
            max_workers=max_workers,
        )

    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "ConsumerRecord") -> None:
        await self._put_msg(msg)


class ConcurrentBetweenPartitionsSubscriber(DefaultSubscriber):
    consumer_subgroup: Iterable["AIOKafkaConsumer"]
    topics: str

    def __init__(
        self,
        topic: str,
        # Kafka information
        group_id: Optional[str],
        listener: Optional["ConsumerRebalanceListener"],
        pattern: Optional[str],
        connection_args: "AnyDict",
        partitions: Iterable["TopicPartition"],
        is_manual: bool,
        # Subscriber args
        max_workers: int,
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[ConsumerRecord]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            topic,
            group_id=group_id,
            listener=listener,
            pattern=pattern,
            connection_args=connection_args,
            partitions=partitions,
            is_manual=is_manual,
            # Propagated args
            no_ack=no_ack,
            no_reply=no_reply,
            retry=retry,
            broker_middlewares=broker_middlewares,
            broker_dependencies=broker_dependencies,
            # AsyncAPI args
            title_=title_,
            description_=description_,
            include_in_schema=include_in_schema,
        )
        self.max_workers = max_workers

    async def start(self) -> None:
        """Start the consumer subgroup."""
        assert self.builder, "You should setup subscriber at first."  # nosec B101

        self.consumer_subgroup = [
            self.builder(
                group_id=self.group_id,
                client_id=self.client_id,
                **self._connection_args,
            )
            for _ in range(self.max_workers)
        ]

        [
            consumer.subscribe(
                topics=self.topics,
                listener=LoggingListenerProxy(
                    consumer=consumer, logger=self.logger, listener=self._listener
                ),
            )
            for consumer in self.consumer_subgroup
        ]

        async with anyio.create_task_group() as tg:
            for consumer in self.consumer_subgroup:
                tg.start_soon(consumer.start)

        self.running = True

        if self.calls:
            for consumer in self.consumer_subgroup:
                self.add_task(self._run_consume_loop(consumer))

    async def stop(self) -> None:
        if self.consumer_subgroup:
            async with anyio.create_task_group() as tg:
                for consumer in self.consumer_subgroup:
                    tg.start_soon(consumer.stop)

            self.consumer_subgroup = []

        await super().stop()

    async def get_msg(self, consumer: "AIOKafkaConsumer") -> "KafkaRawMessage":
        assert consumer, "You should setup subscriber at first."  # nosec B101
        message = await consumer.getone()
        message.consumer = consumer
        return cast("KafkaRawMessage", message)
