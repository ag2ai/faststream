import re
from datetime import datetime
from typing import TYPE_CHECKING, Any, Callable, Dict, Optional, Tuple
from unittest.mock import AsyncMock, MagicMock

import anyio
from aiokafka import ConsumerRecord
from typing_extensions import override

from faststream.broker.message import encode_message, gen_cor_id
from faststream.broker.utils import resolve_custom_func
from faststream.exceptions import SubscriberNotFound
from faststream.kafka import TopicPartition
from faststream.kafka.broker import KafkaBroker
from faststream.kafka.message import KafkaMessage
from faststream.kafka.parser import AioKafkaParser
from faststream.kafka.publisher.asyncapi import AsyncAPIBatchPublisher
from faststream.kafka.publisher.producer import AioKafkaFastProducer
from faststream.kafka.subscriber.asyncapi import AsyncAPIBatchSubscriber
from faststream.testing.broker import TestBroker
from faststream.utils.functions import timeout_scope

if TYPE_CHECKING:
    from faststream.kafka.publisher.asyncapi import AsyncAPIPublisher
    from faststream.kafka.subscriber.usecase import LogicSubscriber
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
            if _is_handler_matches(handler, publisher.topic, publisher.partition):
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
                )
            else:
                sub = broker.subscriber(
                    publisher.topic,
                    batch=isinstance(publisher, AsyncAPIBatchPublisher),
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
        reply_to: str = "",
        rpc: bool = False,
        rpc_timeout: Optional[float] = None,
        raise_timeout: bool = False,
        no_confirm: bool = False,
    ) -> Optional[Any]:
        """Publish a message to the Kafka broker."""
        incoming = build_message(
            message=message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
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
    ) -> "ConsumerRecord":
        incoming = build_message(
            message=message,
            topic=topic,
            key=key,
            partition=partition,
            timestamp_ms=timestamp_ms,
            headers=headers,
            correlation_id=correlation_id,
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
                        correlation_id=correlation_id,
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
    headers: Optional[Dict[str, str]] = None,
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
        key=k,
        serialized_key_size=len(k),
        serialized_value_size=len(msg),
        checksum=sum(msg),
        offset=0,
        headers=[(i, j.encode()) for i, j in headers.items()],
        timestamp_type=1,
        timestamp=timestamp_ms or int(datetime.now().timestamp() * 1000),
    )


def _fake_connection(*args: Any, **kwargs: Any) -> AsyncMock:
    mock = AsyncMock()
    mock.subscribe = MagicMock
    mock.assign = MagicMock
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
        or (handler._pattern and re.match(handler._pattern, topic))
    )
