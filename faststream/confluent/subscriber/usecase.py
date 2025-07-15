from abc import abstractmethod
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
)

import anyio
from confluent_kafka import KafkaException, Message
from typing_extensions import override

from faststream.broker.publisher.fake import FakePublisher
from faststream.broker.subscriber.mixins import ConcurrentMixin, TasksMixin
from faststream.broker.subscriber.usecase import SubscriberUsecase
from faststream.broker.types import MsgType
from faststream.broker.utils import process_msg
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.schemas import TopicPartition

if TYPE_CHECKING:
    from fast_depends.dependencies import Depends

    from faststream.broker.message import StreamMessage
    from faststream.broker.publisher.proto import ProducerProto
    from faststream.broker.types import (
        AsyncCallable,
        BrokerMiddleware,
        CustomCallable,
    )
    from faststream.confluent.client import AsyncConfluentConsumer
    from faststream.types import AnyDict, Decorator, LoggerProto


class LogicSubscriber(TasksMixin, SubscriberUsecase[MsgType]):
    """A class to handle logic for consuming messages from Kafka."""

    topics: Sequence[str]
    group_id: Optional[str]

    builder: Optional[Callable[..., "AsyncConfluentConsumer"]]
    consumer: Optional["AsyncConfluentConsumer"]

    client_id: Optional[str]

    def __init__(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        # Kafka information
        group_id: Optional[str],
        connection_data: "AnyDict",
        is_manual: bool,
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

        self.__connection_data = connection_data

        self.group_id = group_id
        self.topics = topics
        self.partitions = partitions
        self.is_manual = is_manual

        self.consumer = None
        self.polling_interval = polling_interval

        # Setup it later
        self.client_id = ""
        self.builder = None

    @override
    def setup(  # type: ignore[override]
        self,
        *,
        client_id: Optional[str],
        builder: Callable[..., "AsyncConfluentConsumer"],
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
        await consumer.start()

        await super().start()

        if self.calls:
            self.add_task(self._consume())

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

        raw_message = await self.consumer.getone(timeout=timeout)

        return await process_msg(
            msg=raw_message,  # type: ignore[arg-type]
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

    async def consume_one(self, msg: MsgType) -> None:
        await self.consume(msg)

    @abstractmethod
    async def get_msg(self) -> Optional[MsgType]:
        raise NotImplementedError()

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
    def topic_names(self) -> List[str]:
        if self.topics:
            return list(self.topics)
        else:
            return [f"{p.topic}-{p.partition}" for p in self.partitions]

    @staticmethod
    def get_routing_hash(topics: Iterable[str], group_id: Optional[str] = None) -> int:
        return hash("".join((*topics, group_id or "")))

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
        is_manual: bool,
        # Subscriber args
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[Message]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            connection_data=connection_data,
            is_manual=is_manual,
            # subscriber args
            default_parser=AsyncConfluentParser.parse_message,
            default_decoder=AsyncConfluentParser.decode_message,
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

    async def get_msg(self) -> Optional["Message"]:
        assert self.consumer, "You should setup subscriber at first."  # nosec B101
        return await self.consumer.getone(timeout=self.polling_interval)

    def get_log_context(
        self,
        message: Optional["StreamMessage[Message]"],
    ) -> Dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message.topic() or ",".join(self.topics)

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class BatchSubscriber(LogicSubscriber[Tuple[Message, ...]]):
    def __init__(
        self,
        *topics: str,
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        max_records: Optional[int],
        # Kafka information
        group_id: Optional[str],
        connection_data: "AnyDict",
        is_manual: bool,
        # Subscriber args
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[Tuple[Message, ...]]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        self.max_records = max_records

        super().__init__(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            connection_data=connection_data,
            is_manual=is_manual,
            # subscriber args
            default_parser=AsyncConfluentParser.parse_message_batch,
            default_decoder=AsyncConfluentParser.decode_message_batch,
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

    async def get_msg(self) -> Optional[Tuple["Message", ...]]:
        assert self.consumer, "You should setup subscriber at first."  # nosec B101

        messages = await self.consumer.getmany(
            timeout=self.polling_interval,
            max_records=self.max_records,
        )

        if not messages:  # TODO: why we are sleeping here?
            await anyio.sleep(self.polling_interval)
            return None

        return messages

    def get_log_context(
        self,
        message: Optional["StreamMessage[Tuple[Message, ...]]"],
    ) -> Dict[str, str]:
        if message is None:
            topic = ",".join(self.topic_names)
        else:
            topic = message.raw_message[0].topic() or ",".join(self.topic_names)

        return self.build_log_context(
            message=message,
            topic=topic,
            group_id=self.group_id,
        )


class ConcurrentDefaultSubscriber(ConcurrentMixin[Message], DefaultSubscriber):
    def __init__(
        self,
        *topics: str,
        # Kafka information
        partitions: Sequence["TopicPartition"],
        polling_interval: float,
        group_id: Optional[str],
        connection_data: "AnyDict",
        is_manual: bool,
        # Subscriber args
        max_workers: int,
        no_ack: bool,
        no_reply: bool,
        retry: bool,
        broker_dependencies: Iterable["Depends"],
        broker_middlewares: Sequence["BrokerMiddleware[Message]"],
        # AsyncAPI args
        title_: Optional[str],
        description_: Optional[str],
        include_in_schema: bool,
    ) -> None:
        super().__init__(
            *topics,
            partitions=partitions,
            polling_interval=polling_interval,
            group_id=group_id,
            connection_data=connection_data,
            is_manual=is_manual,
            # subscriber args
            max_workers=max_workers,
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

    async def start(self) -> None:
        await super().start()
        self.start_consume_task()

    async def consume_one(self, msg: "Message") -> None:
        await self._put_msg(msg)
