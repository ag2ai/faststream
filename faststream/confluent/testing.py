from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, List, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import anyio
from typing_extensions import override

from faststream.broker.message import encode_message, gen_cor_id
from faststream.broker.utils import resolve_custom_func
from faststream.confluent.broker import KafkaBroker
from faststream.confluent.parser import AsyncConfluentParser
from faststream.confluent.publisher.asyncapi import AsyncAPIBatchPublisher
from faststream.confluent.publisher.producer import AsyncConfluentFastProducer
from faststream.confluent.schemas import TopicPartition
from faststream.confluent.subscriber.asyncapi import AsyncAPIBatchSubscriber
from faststream.exceptions import SubscriberNotFound
from faststream.testing.broker import TestBroker
from faststream.utils.functions import timeout_scope

if TYPE_CHECKING:
    from faststream.confluent.publisher.asyncapi import AsyncAPIPublisher
    from faststream.confluent.subscriber.usecase import LogicSubscriber
    from faststream.types import SendableMessage

__all__ = ("TestKafkaBroker",)


class TestKafkaBroker(TestBroker[KafkaBroker]):
    """A class to test Kafka brokers."""

    @staticmethod
    async def _fake_connect(  # type: ignore[override]
        broker: KafkaBroker,
        *args: Any,
        **kwargs: Any,
    ) -> Callable[..., AsyncMock]:
        broker._producer = FakeProducer(broker)
        return _fake_connection

    @staticmethod
    def create_publisher_fake_subscriber(
        broker: KafkaBroker,
        publisher: "AsyncAPIPublisher[Any]",
    ) -> Tuple["LogicSubscriber[Any]", bool]:
        sub: Optional[LogicSubscriber[Any]] = None
        for handler in broker._subscribers.values():
            if _is_handler_matches(
                handler, topic=publisher.topic, partition=publisher.partition
            ):
                sub = handler
                break

        if sub is None:
            is_real = False

            if publisher.partition:
                tp = TopicPartition(
                    topic=publisher.topic, partition=publisher.partition
                )
                sub = broker.subscriber(
                    partitions=[tp],
                    batch=isinstance(publisher, AsyncAPIBatchPublisher),
                    auto_offset_reset="earliest",
                )
            else:
                sub = broker.subscriber(
                    publisher.topic,
                    batch=isinstance(publisher, AsyncAPIBatchPublisher),
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

        default = AsyncConfluentParser

        self._parser = resolve_custom_func(broker._parser, default.parse_message)
        self._decoder = resolve_custom_func(broker._decoder, default.decode_message)

    @override
    async def publish(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        *,
        no_confirm: bool = False,
        reply_to: str = "",
        rpc: bool = False,
        rpc_timeout: Optional[float] = None,
        raise_timeout: bool = False,
    ) -> Optional[Any]:
        """Publish a message to the Kafka broker."""
        incoming = build_message(
            message=message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
            reply_to=reply_to,
        )

        return_value = None

        for handler in self.broker._subscribers.values():  # pragma: no branch
            if _is_handler_matches(handler, topic, partition):
                msg_to_send = (
                    [incoming]
                    if isinstance(handler, AsyncAPIBatchSubscriber)
                    else incoming
                )

                with timeout_scope(rpc_timeout, raise_timeout):
                    response_msg = await self._execute_handler(
                        msg_to_send, topic, handler
                    )
                    if rpc:
                        return_value = return_value or await self._decoder(
                            await self._parser(response_msg)
                        )

        return return_value

    async def publish_batch(
        self,
        *msgs: "SendableMessage",
        topic: str,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        reply_to: str = "",
        correlation_id: Optional[str] = None,
        no_confirm: bool = False,
    ) -> None:
        """Publish a batch of messages to the Kafka broker."""
        for handler in self.broker._subscribers.values():  # pragma: no branch
            if _is_handler_matches(handler, topic, partition):
                messages = (
                    build_message(
                        message=message,
                        topic=topic,
                        partition=partition,
                        timestamp_ms=timestamp_ms,
                        headers=headers,
                        correlation_id=correlation_id or gen_cor_id(),
                        reply_to=reply_to,
                    )
                    for message in msgs
                )

                if isinstance(handler, AsyncAPIBatchSubscriber):
                    await self._execute_handler(list(messages), topic, handler)

                else:
                    for m in messages:
                        await self._execute_handler(m, topic, handler)

        return None

    @override
    async def request(  # type: ignore[override]
        self,
        message: "SendableMessage",
        topic: str,
        key: Optional[bytes] = None,
        partition: Optional[int] = None,
        timestamp_ms: Optional[int] = None,
        headers: Optional[Dict[str, str]] = None,
        correlation_id: Optional[str] = None,
        *,
        timeout: Optional[float] = 0.5,
    ) -> "MockConfluentMessage":
        incoming = build_message(
            message=message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id or gen_cor_id(),
        )

        for handler in self.broker._subscribers.values():  # pragma: no branch
            if _is_handler_matches(handler, topic, partition):
                msg_to_send = (
                    [incoming]
                    if isinstance(handler, AsyncAPIBatchSubscriber)
                    else incoming
                )

                with anyio.fail_after(timeout):
                    return await self._execute_handler(msg_to_send, topic, handler)

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
        headers: List[Tuple[str, bytes]],
        offset: int,
        partition: int,
        timestamp_type: int,
        timestamp_ms: int,
        error: Optional[str] = None,
    ):
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

    def headers(self) -> List[Tuple[str, bytes]]:
        return self._headers

    def key(self) -> bytes:
        return self._key

    def offset(self) -> int:
        return self._offset

    def partition(self) -> int:
        return self._partition

    def timestamp(self) -> Tuple[int, int]:
        return self._timestamp

    def topic(self) -> str:
        return self._topic

    def value(self) -> bytes:
        return self._raw_msg


def build_message(
    message: "SendableMessage",
    topic: str,
    *,
    correlation_id: str,
    partition: Optional[int] = None,
    timestamp_ms: Optional[int] = None,
    key: Optional[bytes] = None,
    headers: Optional[Dict[str, str]] = None,
    reply_to: str = "",
) -> MockConfluentMessage:
    """Build a mock confluent_kafka.Message for a sendable message."""
    msg, content_type = encode_message(message)
    k = key or b""
    headers = {
        "content-type": content_type or "",
        "correlation_id": correlation_id,
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
        timestamp_type=1,
        timestamp_ms=timestamp_ms or int(datetime.now().timestamp() * 1000),
    )


def _fake_connection(*args: Any, **kwargs: Any) -> AsyncMock:
    mock = AsyncMock()
    mock.getone.return_value = MagicMock()
    mock.getmany.return_value = [MagicMock()]
    return mock


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
    )
