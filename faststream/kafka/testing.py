import re
from collections.abc import Callable, Generator, Iterable, Iterator
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Any, Optional, cast
from unittest.mock import AsyncMock, MagicMock

import anyio
from aiokafka import ConsumerRecord
from typing_extensions import override

from faststream._internal.endpoint.utils import ParserComposition
from faststream._internal.testing.broker import TestBroker, change_producer
from faststream.exceptions import SubscriberNotFound
from faststream.kafka import TopicPartition
from faststream.kafka.broker import KafkaBroker
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser
from faststream.kafka.publisher.producer import AioKafkaFastProducer
from faststream.kafka.publisher.usecase import BatchPublisher
from faststream.kafka.subscriber.usecase import BatchSubscriber
from faststream.message import encode_message, gen_cor_id

if TYPE_CHECKING:
    from fast_depends.library.serializer import SerializerProto

    from faststream._internal.basic_types import SendableMessage
    from faststream.kafka.publisher.usecase import LogicPublisher
    from faststream.kafka.response import KafkaPublishCommand
    from faststream.kafka.subscriber.usecase import LogicSubscriber

__all__ = ("TestKafkaBroker",)


class TestKafkaBroker(TestBroker[KafkaBroker]):
    """A class to test Kafka brokers."""

    @contextmanager
    def _patch_producer(self, broker: KafkaBroker) -> Iterator[None]:
        fake_producer = FakeProducer(broker)

        with ExitStack() as es:
            es.enter_context(
                change_producer(broker.config.broker_config, fake_producer),
            )
            yield

    @staticmethod
    async def _fake_connect(  # type: ignore[override]
        broker: KafkaBroker,
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., AsyncMock]:
        broker.config.broker_config._admin_client = AsyncMock()

        builder = MagicMock(return_value=FakeConsumer())
        broker.config.broker_config.builder = builder

        return _fake_connection

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: KafkaBroker,
        publisher: "LogicPublisher",
    ) -> tuple["LogicSubscriber[Any]", bool]:
        sub: LogicSubscriber[Any] | None = None
        for handler in broker.subscribers:
            handler = cast("LogicSubscriber[Any]", handler)
            if _is_handler_matches(handler, publisher.topic, publisher.partition):
                sub = handler
                break

        if sub is None:
            is_real = False

            topic_name = publisher.topic

            if publisher.partition:
                tp = TopicPartition(
                    topic=topic_name,
                    partition=publisher.partition,
                )
                sub = broker.subscriber(
                    partitions=[tp],
                    batch=isinstance(publisher, BatchPublisher),
                    persistent=False,
                )
            else:
                sub = broker.subscriber(
                    topic_name,
                    batch=isinstance(publisher, BatchPublisher),
                    persistent=False,
                )
        else:
            is_real = True

        return sub, is_real


class FakeConsumer:
    async def start(self) -> None:
        pass

    async def stop(self) -> None:
        pass

    def subscribe(self, *args: Any, **kwargs: Any) -> None:
        pass


class FakeProducer(AioKafkaFastProducer):
    """A fake Kafka producer for testing purposes.

    This class extends AioKafkaFastProducer and is used to simulate Kafka message publishing during tests.
    """

    def __init__(self, broker: KafkaBroker) -> None:
        self.broker = broker

        default = AioKafkaParser(
            msg_class=KafkaMessage,
            regex=None,
        )

        self._parser = ParserComposition(broker._parser, default.parse_message)
        self._decoder = ParserComposition(broker._decoder, default.decode_message)

    def __bool__(self) -> bool:
        return True

    @property
    def closed(self) -> bool:
        return False

    @override
    async def publish(self, cmd: "KafkaPublishCommand") -> None:
        """Publish a message to the Kafka broker."""
        incoming = build_message(
            message=cmd.body,
            topic=cmd.destination,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            reply_to=cmd.reply_to,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in _find_handler(
            cast("list[LogicSubscriber[Any]]", self.broker.subscribers),
            cmd.destination,
            cmd.partition,
        ):
            msg_to_send = [incoming] if isinstance(handler, BatchSubscriber) else incoming

            await self._execute_handler(msg_to_send, cmd.destination, handler)

    @override
    async def request(self, cmd: "KafkaPublishCommand") -> "ConsumerRecord":
        incoming = build_message(
            message=cmd.body,
            topic=cmd.destination,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
            serializer=self.broker.config.fd_config._serializer,
        )

        for handler in _find_handler(
            cast("list[LogicSubscriber[Any]]", self.broker.subscribers),
            cmd.destination,
            cmd.partition,
        ):
            msg_to_send = [incoming] if isinstance(handler, BatchSubscriber) else incoming

            with anyio.fail_after(cmd.timeout):
                return await self._execute_handler(
                    msg_to_send,
                    cmd.destination,
                    handler,
                )

        raise SubscriberNotFound

    @override
    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> None:
        """Publish a batch of messages to the Kafka broker."""
        for handler in _find_handler(
            cast("list[LogicSubscriber[Any]]", self.broker.subscribers),
            cmd.destination,
            cmd.partition,
        ):
            messages = (
                build_message(
                    message=message,
                    topic=cmd.destination,
                    partition=cmd.partition,
                    timestamp_ms=cmd.timestamp_ms,
                    key=cmd.key_for(message_position),
                    headers=cmd.headers,
                    correlation_id=cmd.correlation_id,
                    reply_to=cmd.reply_to,
                    serializer=self.broker.config.fd_config._serializer,
                )
                for message_position, message in enumerate(cmd.batch_bodies)
            )

            if isinstance(handler, BatchSubscriber):
                await self._execute_handler(list(messages), cmd.destination, handler)

            else:
                for m in messages:
                    await self._execute_handler(m, cmd.destination, handler)

    async def _execute_handler(
        self,
        msg: Any,
        topic: str,
        handler: "LogicSubscriber[Any]",
    ) -> "ConsumerRecord":
        result = await handler.process_message(msg)

        return build_message(
            topic=topic,
            message=result.body,
            headers=result.headers,
            correlation_id=result.correlation_id,
            serializer=self.broker.config.fd_config._serializer,
        )


def build_message(
    message: "SendableMessage",
    topic: str,
    partition: int | None = None,
    timestamp_ms: int | None = None,
    key: bytes | None = None,
    headers: dict[str, str] | None = None,
    correlation_id: str | None = None,
    *,
    reply_to: str = "",
    serializer: Optional["SerializerProto"],
) -> "ConsumerRecord":
    """Build a Kafka ConsumerRecord for a sendable message."""
    msg, content_type = encode_message(message, serializer=serializer)

    k = key or b""

    headers = {
        "content-type": content_type or "",
        "correlation_id": correlation_id or gen_cor_id(),
        **(headers or {}),
    }

    if reply_to:
        headers["reply_to"] = headers.get("reply_to", reply_to)

    return ConsumerRecord(
        value=msg,
        topic=topic,
        partition=partition or 0,
        key=k,
        serialized_key_size=len(k),
        serialized_value_size=len(msg),
        checksum=sum(msg),
        offset=0,
        headers=[(i, j.encode()) for i, j in headers.items()],
        timestamp_type=1,
        timestamp=timestamp_ms or int(datetime.now(timezone.utc).timestamp() * 1000),
    )


def _fake_connection(*args: Any, **kwargs: Any) -> AsyncMock:
    mock = AsyncMock()
    mock.subscribe = MagicMock
    mock.assign = MagicMock
    return mock


def _find_handler(
    subscribers: Iterable["LogicSubscriber[Any]"],
    topic: str,
    partition: int | None,
) -> Generator["LogicSubscriber[Any]", None, None]:
    published_groups = set()
    for handler in subscribers:  # pragma: no branch
        if _is_handler_matches(handler, topic, partition):
            if handler.group_id:
                if handler.group_id in published_groups:
                    continue
                else:
                    published_groups.add(handler.group_id)
            yield handler


def _is_handler_matches(
    handler: "LogicSubscriber[Any]",
    topic: str,
    partition: int | None,
) -> bool:
    return bool(
        any(
            p.topic == topic and (partition is None or p.partition == partition)
            for p in handler.partitions
        )
        or topic in handler.topics
        or (handler.pattern and re.match(handler.pattern, topic)),
    )
