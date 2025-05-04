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
from typing_extensions import override

from faststream._internal.subscriber.utils import resolve_custom_func
from faststream._internal.testing.broker import TestBroker
from faststream.confluent.broker import KafkaBroker
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.publisher.producer import AsyncConfluentFastProducer
from faststream.confluent.publisher.specified import SpecificationBatchPublisher
from faststream.confluent.schemas import TopicPartition
from faststream.confluent.subscriber.usecase import BatchSubscriber
from faststream.exceptions import SubscriberNotFound
from faststream.message import encode_message, gen_cor_id

if TYPE_CHECKING:
    from faststream._internal.basic_types import SendableMessage
    from faststream.confluent.publisher.specified import SpecificationPublisher
    from faststream.confluent.response import KafkaPublishCommand
    from faststream.confluent.subscriber.usecase import LogicSubscriber


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
            if _is_handler_matches(
                handler,
                topic=publisher.topic,
                partition=publisher.partition,
            ):
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
                    auto_offset_reset="earliest",
                )
            else:
                sub = broker.subscriber(
                    publisher.topic,
                    batch=isinstance(publisher, SpecificationBatchPublisher),
                    auto_offset_reset="earliest",
                )

        else:
            is_real = True

        return sub, is_real


class FakeProducer(AsyncConfluentFastProducer):
    """A fake Kafka producer for testing purposes.

    This class extends AsyncConfluentFastProducer and is used to simulate Kafka message publishing during tests.
    """

    def __init__(self, broker: KafkaBroker) -> None:
        self.broker = broker

        default = AsyncConfluentParser()

        self._parser = resolve_custom_func(broker._parser, default.parse_message)
        self._decoder = resolve_custom_func(broker._decoder, default.decode_message)

    def __bool__(self) -> bool:
        return True

    async def ping(self, timeout: float) -> None:
        return True

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

    @override
    async def request(  # type: ignore[override]
        self,
        cmd: "KafkaPublishCommand",
    ) -> "MockConfluentMessage":
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

    async def _execute_handler(
        self,
        msg: Any,
        topic: str,
        handler: "LogicSubscriber[Any]",
    ) -> "MockConfluentMessage":
        result = await handler.process_message(msg)

        return build_message(
            topic=topic,
            message=result.body,
            headers=result.headers,
            correlation_id=result.correlation_id or gen_cor_id(),
        )


class MockConfluentMessage:
    def __init__(
        self,
        raw_msg: bytes,
        topic: str,
        key: bytes,
        headers: list[tuple[str, bytes]],
        offset: int,
        partition: int,
        timestamp_type: int,
        timestamp_ms: int,
        error: Optional[str] = None,
    ) -> None:
        self._raw_msg = raw_msg
        self._topic = topic
        self._key = key
        self._headers = headers
        self._error = error
        self._offset = offset
        self._partition = partition
        self._timestamp = (timestamp_type, timestamp_ms)

    def len(self) -> int:
        return len(self._raw_msg)

    def error(self) -> Optional[str]:
        return self._error

    def headers(self) -> list[tuple[str, bytes]]:
        return self._headers

    def key(self) -> bytes:
        return self._key

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return self._partition

    def timestamp(self) -> tuple[int, int]:
        return self._timestamp

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._raw_msg


def build_message(
    message: "SendableMessage",
    topic: str,
    *,
    correlation_id: Optional[str] = None,
    partition: Optional[int] = None,
    timestamp_ms: Optional[int] = None,
    key: Optional[bytes] = None,
    headers: Optional[dict[str, str]] = None,
    reply_to: str = "",
) -> MockConfluentMessage:
    """Build a mock confluent_kafka.Message for a sendable message."""
    msg, content_type = encode_message(message)
    k = key or b""
    headers = {
        "content-type": content_type or "",
        "correlation_id": correlation_id or gen_cor_id(),
        "reply_to": reply_to,
        **(headers or {}),
    }

    # https://docs.confluent.io/platform/current/clients/confluent-kafka-python/html/index.html#confluent_kafka.Message.timestamp
    return MockConfluentMessage(
        raw_msg=msg,
        topic=topic,
        key=k,
        headers=[(i, j.encode()) for i, j in headers.items()],
        offset=0,
        partition=partition or 0,
        timestamp_type=0 + 1,
        timestamp_ms=timestamp_ms or int(datetime.now(timezone.utc).timestamp()),
    )


def _fake_connection(*args: Any, **kwargs: Any) -> AsyncMock:
    mock = AsyncMock()
    mock.getone.return_value = MagicMock()
    mock.getmany.return_value = [MagicMock()]
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
        or topic in handler.topics,
    )
