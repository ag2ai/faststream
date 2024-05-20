import asyncio
from typing import Any, ClassVar, Dict
from unittest.mock import patch

import pytest

from faststream.confluent import KafkaBroker
from faststream.confluent.annotations import KafkaMessage
from faststream.confluent.client import AsyncConfluentConsumer
from faststream.exceptions import AckMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator


@pytest.mark.confluent()
class TestConsume(BrokerRealConsumeTestcase):
    """A class to represent a test Kafka broker."""

    timeout: int = 10
    subscriber_kwargs: ClassVar[Dict[str, Any]] = {"auto_offset_reset": "earliest"}

    def get_broker(self, apply_types: bool = False):
        return KafkaBroker(apply_types=apply_types)

    @pytest.mark.asyncio()
    async def test_consume_batch(self, queue: str):
        consume_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=1)

        @consume_broker.subscriber(queue, batch=True, **self.subscriber_kwargs)
        async def handler(msg):
            await msgs_queue.put(msg)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await br.publish_batch(1, "hi", topic=queue)

            result, _ = await asyncio.wait(
                (asyncio.create_task(msgs_queue.get()),),
                timeout=self.timeout,
            )

        assert [{1, "hi"}] == [set(r.result()) for r in result]

    @pytest.mark.asyncio()
    async def test_consume_batch_headers(
        self,
        mock,
        event: asyncio.Event,
        queue: str,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(queue, batch=True, **self.subscriber_kwargs)
        def subscriber(m, msg: KafkaMessage):
            check = all(
                (
                    msg.headers,
                    [msg.headers] == msg.batch_headers,
                    msg.headers.get("custom") == "1",
                )
            )
            mock(check)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, headers={"custom": "1"})),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with(True)

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_ack(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", auto_commit=False, **self.subscriber_kwargs
        )
        async def handler(msg: KafkaMessage):
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            )
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_ack_manual(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", auto_commit=False, **self.subscriber_kwargs
        )
        async def handler(msg: KafkaMessage):
            await msg.ack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_ack_raise(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", auto_commit=False, **self.subscriber_kwargs
        )
        async def handler(msg: KafkaMessage):
            event.set()
            raise AckMessage()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                m.mock.assert_called_once()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_nack(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", auto_commit=False, **self.subscriber_kwargs
        )
        async def handler(msg: KafkaMessage):
            await msg.nack()
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                assert not m.mock.called

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_no_ack(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, group_id="test", no_ack=True, **self.subscriber_kwargs
        )
        async def handler(msg: KafkaMessage):
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(
                AsyncConfluentConsumer,
                "commit",
                spy_decorator(AsyncConfluentConsumer.commit),
            ) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(
                            br.publish(
                                "hello",
                                queue,
                            )
                        ),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                m.mock.assert_not_called()

        assert event.is_set()

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_consume_with_no_auto_commit(
        self,
        queue: str,
        event: asyncio.Event,
    ):
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            queue, auto_commit=False, group_id="test", **self.subscriber_kwargs
        )
        async def subscriber_no_auto_commit(msg: KafkaMessage):
            await msg.nack()
            event.set()

        broker2 = self.get_broker()
        event2 = asyncio.Event()

        @broker2.subscriber(
            queue, auto_commit=True, group_id="test", **self.subscriber_kwargs
        )
        async def subscriber_with_auto_commit(m):
            event2.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        async with self.patch_broker(broker2) as br2:
            await br2.start()

            await asyncio.wait(
                (asyncio.create_task(event2.wait()),),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
