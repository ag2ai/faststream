import re
from collections.abc import Generator, Iterable, Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    Optional,
)
from unittest.mock import AsyncMock, MagicMock

import anyio
from aiokafka import ConsumerRecord
from typing_extensions import override

from faststream._internal.subscriber.utils import resolve_custom_func
from faststream._internal.testing.broker import TestBroker
from faststream.exceptions import SubscriberNotFound
from faststream.kafka import TopicPartition
from faststream.kafka.broker import KafkaBroker
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser
from faststream.kafka.publisher.producer import AioKafkaFastProducer
from faststream.kafka.publisher.specified import SpecificationBatchPublisher
from faststream.kafka.subscriber.usecase import BatchSubscriber
from faststream.message import encode_message, gen_cor_id

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.kafka.publisher.specified import SpecificationPublisher
    from faststream.kafka.response import KafkaPublishCommand
    from faststream.kafka.subscriber.usecase import LogicSubscriber

__all__ = ("TestKafkaBroker",)


class TestKafkaBroker(TestBroker[KafkaBroker]):
    """A class to test Kafka brokers."""

    @contextmanager
    def _patch_producer(self, broker: KafkaBroker) -> Iterator[None]:
        old_producer = broker._state.get().producer
        broker._state.patch_value(producer=FakeProducer(broker))
        yield
        broker._state.patch_value(producer=old_producer)

    @staticmethod
    async def _fake_connect(  # type: ignore[override]
        broker: KafkaBroker,
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., AsyncMock]:
        return _fake_connection

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: KafkaBroker,
        publisher: "SpecificationPublisher[Any, Any]",
    ) -> tuple["LogicSubscriber[Any]", bool]:
        sub: Optional[LogicSubscriber[Any]] = None
        for handler in broker._subscribers:
            if _is_handler_matches(handler, publisher.topic, publisher.partition):
                sub = handler
                break

        if sub is None:
            is_real = False

            if publisher.partition:
                tp = TopicPartition(
                    topic=publisher.topic,
                    partition=publisher.partition,
                )
                sub = broker.subscriber(
                    partitions=[tp],
                    batch=isinstance(publisher, SpecificationBatchPublisher),
                )
            else:
                sub = broker.subscriber(
                    publisher.topic,
                    batch=isinstance(publisher, SpecificationBatchPublisher),
                )
        else:
            is_real = True

        return sub, is_real


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

        self._parser = resolve_custom_func(broker._parser, default.parse_message)
        self._decoder = resolve_custom_func(broker._decoder, default.decode_message)

    def __bool__(self) -> None:
        return True

    @property
    def closed(self) -> bool:
        return False

    @override
    async def publish(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> None:
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
        )

        for handler in _find_handler(
            self.broker._subscribers,
            cmd.destination,
            cmd.partition,
        ):
            msg_to_send = (
                [incoming] if isinstance(handler, BatchSubscriber) else incoming
            )

            await self._execute_handler(msg_to_send, cmd.destination, handler)

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> "ConsumerRecord":
        incoming = build_message(
            message=cmd.body,
            topic=cmd.destination,
            key=cmd.key,
            partition=cmd.partition,
            timestamp_ms=cmd.timestamp_ms,
            headers=cmd.headers,
            correlation_id=cmd.correlation_id,
        )

        for handler in _find_handler(
            self.broker._subscribers,
            cmd.destination,
            cmd.partition,
        ):
            msg_to_send = (
                [incoming] if isinstance(handler, BatchSubscriber) else incoming
            )

            with anyio.fail_after(cmd.timeout):
                return await self._execute_handler(
                    msg_to_send, cmd.destination, handler
                )

        raise SubscriberNotFound

    async def publish_batch(
        self,
        cmd: "KafkaPublishCommand",
    ) -> None:
        """Publish a batch of messages to the Kafka broker."""
        for handler in _find_handler(
            self.broker._subscribers,
            cmd.destination,
            cmd.partition,
        ):
            messages = (
                build_message(
                    message=message,
                    topic=cmd.destination,
                    partition=cmd.partition,
                    timestamp_ms=cmd.timestamp_ms,
                    headers=cmd.headers,
                    correlation_id=cmd.correlation_id,
                    reply_to=cmd.reply_to,
                )
                for message in cmd.batch_bodies
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
        )


def build_message(
    message: "SendableMessage",
    topic: str,
    partition: Optional[int] = None,
    timestamp_ms: Optional[int] = None,
    key: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    correlation_id: Optional[str] = None,
    *,
    reply_to: str = "",
) -> "ConsumerRecord":
    """Build a Kafka ConsumerRecord for a sendable message."""
    msg, content_type = encode_message(message)

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
        timestamp=timestamp_ms or int(datetime.now(timezone.utc).timestamp()),
        timestamp_type=0,
        key=k,
        serialized_key_size=len(k),
        serialized_value_size=len(msg),
        checksum=sum(msg),
        offset=0,
        headers=[(i, j.encode()) for i, j in headers.items()],
    )


def _fake_connection(*args: Any, **kwargs: Any) -> AsyncMock:
    mock = AsyncMock()
    mock.subscribe = MagicMock
    mock.assign = MagicMock
    return mock


def _find_handler(
    subscribers: Iterable["LogicSubscriber[Any]"],
    topic: str,
    partition: Optional[int],
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
    partition: Optional[int],
) -> bool:
    return bool(
        any(
            p.topic == topic and (partition is None or p.partition == partition)
            for p in handler.partitions
        )
        or topic in handler.topics
        or (handler._pattern and re.match(handler._pattern, topic)),
    )
