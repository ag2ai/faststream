import asyncio
from unittest.mock import MagicMock, patch

import pytest

from faststream import AckPolicy
from faststream.confluent.annotations import KafkaMessage
from faststream.confluent.client import AsyncConfluentConsumer
from faststream.exceptions import AckMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import ConfluentTestcaseConfig


@pytest.mark.confluent()
class TestConsume(ConfluentTestcaseConfig, BrokerRealConsumeTestcase):
    """A class to represent a test Kafka broker."""

    @pytest.mark.asyncio()
    async def test_consume_batch(self, queue: str) -> None:
        consume_broker = self.get_broker()

        msgs_queue = asyncio.Queue(maxsize=1)

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
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
        mock: MagicMock,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue, batch=True)

        @consume_broker.subscriber(*args, **kwargs)
        def subscriber(m, msg: KafkaMessage) -> None:
            check = all(
                (
                    msg.headers,
                    [msg.headers] == msg.batch_headers,
                    msg.headers.get("custom") == "1",
                ),
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
    async def test_consume_auto_ack(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
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
                            ),
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
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
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
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage):
            event.set()
            raise AckMessage

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
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
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
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue, group_id="test", ack_policy=AckPolicy.DO_NOTHING
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg: KafkaMessage) -> None:
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
                            ),
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
    ) -> None:
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @consume_broker.subscriber(*args, **kwargs)
        async def subscriber_no_auto_commit(msg: KafkaMessage) -> None:
            await msg.nack()
            event.set()

        broker2 = self.get_broker()
        event2 = asyncio.Event()

        args, kwargs = self.get_subscriber_params(
            queue,
            group_id="test",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )

        @broker2.subscriber(*args, **kwargs)
        async def subscriber_with_auto_commit(m) -> None:
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

    @pytest.mark.asyncio()
    @pytest.mark.slow()
    async def test_concurrent_consume(self, queue: str, mock: MagicMock) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        consume_broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, max_workers=2)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(msg):
            mock()
            if event.is_set():
                event2.set()
            else:
                event.set()
            await asyncio.sleep(0.1)

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, queue)

        await asyncio.wait(
            (
                asyncio.create_task(event.wait()),
                asyncio.create_task(event2.wait()),
            ),
            timeout=3,
        )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_count
