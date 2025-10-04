import asyncio
from unittest.mock import AsyncMock, patch

import pytest

from faststream import AckPolicy, BaseMiddleware
from faststream.confluent.annotations import KafkaMessage
from faststream.confluent.message import FAKE_CONSUMER
from faststream.confluent.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase
from tests.tools import spy_decorator

from .basic import ConfluentMemoryTestcaseConfig


@pytest.mark.confluent()
@pytest.mark.asyncio()
class TestTestclient(ConfluentMemoryTestcaseConfig, BrokerTestclientTestcase):
    async def test_message_nack_seek(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        @broker.subscriber(
            queue,
            group_id=f"{queue}-consume",
            auto_offset_reset="earliest",
            ack_policy=AckPolicy.REJECT_ON_ERROR,
        )
        async def m(msg: KafkaMessage) -> None:
            await msg.nack()

        async with self.patch_broker(broker) as br:
            with patch.object(
                FAKE_CONSUMER,
                "seek",
                spy_decorator(FAKE_CONSUMER.seek),
            ) as mocked:
                await br.publish("hello", queue)
                m.mock.assert_called_once_with("hello")
                mocked.mock.assert_called_once()

    @pytest.mark.connected()
    async def test_with_real_testclient(self, queue: str) -> None:
        event = asyncio.Event()

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(broker, with_real=True) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=10,
            )

        assert event.is_set()

    async def test_publisher_autoflush_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", autoflush=True)
        publisher.flush = AsyncMock()

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once()

            publisher.flush.assert_awaited_once()

    async def test_batch_publisher_autoflush_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", batch=True, autoflush=True)
        publisher.flush = AsyncMock()

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)

            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

            publisher.flush.assert_awaited_once()

    async def test_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_pub_by_pub_batch(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish_batch("hello", topic=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_publisher_mock(self, queue: str) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "1", batch=True)

        @publisher
        @broker.subscriber(queue)
        async def m(msg):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(msg) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")

        assert len(routes) == 2

    @pytest.mark.connected()
    async def test_real_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        args, kwargs = self.get_subscriber_params(queue)

        @broker.subscriber(*args, **kwargs)
        async def h1(msg) -> None: ...

        args2, kwargs2 = self.get_subscriber_params(queue + "1")

        @broker.subscriber(*args2, **kwargs2)
        async def h2(msg) -> None: ...

        async with self.patch_broker(broker, with_real=True) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")
            await h1.wait_call(10)
            await h2.wait_call(10)

        assert len(routes) == 2

    async def test_multiple_subscribers_different_groups(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, group_id="group2")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            assert subscriber1.mock.call_count == 1
            assert subscriber2.mock.call_count == 1

    async def test_multiple_subscribers_same_group(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, group_id="group1")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            # we can't guarantee the order of calls
            assert {subscriber1.mock.call_count, subscriber2.mock.call_count} == {1, 0}

    async def test_multiple_batch_subscriber_with_different_group(
        self, queue: str
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, batch=True, group_id="group2")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            assert subscriber1.mock.call_count == 1
            assert subscriber2.mock.call_count == 1

    async def test_multiple_batch_subscriber_with_same_group(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber1(msg) -> None: ...

        @broker.subscriber(queue, batch=True, group_id="group1")
        async def subscriber2(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("", queue)

            # we can't guarantee the order of calls
            assert {subscriber1.mock.call_count, subscriber2.mock.call_count} == {1, 0}

    @pytest.mark.connected()
    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    @pytest.mark.connected()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        await super().test_broker_with_real_doesnt_get_patched()

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self, queue: str
    ) -> None:
        await super().test_broker_with_real_patches_publishers_and_subscribers(queue)

    @pytest.mark.xfail(reason="https://github.com/ag2ai/faststream/issues/2513")
    async def test_publisher_without_destination(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2513."""
        broker = self.get_broker()

        # use two publishers to check that we don't have conflicts
        publisher = broker.publisher(topic="")
        another_publisher = broker.publisher(topic="")

        async with self.patch_broker(broker):
            await publisher.publish(None, topic="new-key")
            publisher.mock.assert_called_once()

            await another_publisher.publish(None, topic="new-key")
            another_publisher.mock.assert_called_once()
