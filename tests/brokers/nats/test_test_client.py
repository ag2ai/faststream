import asyncio

import pytest

from faststream import BaseMiddleware
from faststream.nats import (
    ConsumerConfig,
    JStream,
    PullSub,
)
from faststream.nats.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import NatsMemoryTestcaseConfig


@pytest.mark.nats()
@pytest.mark.asyncio()
class TestTestclient(NatsMemoryTestcaseConfig, BrokerTestclientTestcase):
    @pytest.mark.asyncio()
    async def test_stream_publish(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker(apply_types=False)

        @pub_broker.subscriber(queue, stream="test")
        async def m(msg) -> None: ...

        async with self.patch_broker(pub_broker) as br:
            await br.publish("Hi!", queue, stream="test")
            m.mock.assert_called_once_with("Hi!")

    @pytest.mark.asyncio()
    async def test_wrong_stream_publish(
        self,
        queue: str,
    ) -> None:
        pub_broker = self.get_broker(apply_types=False)

        @pub_broker.subscriber(queue)
        async def m(msg) -> None: ...

        async with self.patch_broker(pub_broker) as br:
            await br.publish("Hi!", queue, stream="test")
            assert not m.mock.called

    @pytest.mark.connected()
    async def test_with_real_testclient(
        self,
        queue: str,
    ) -> None:
        event = asyncio.Event()

        broker = self.get_broker()

        @broker.subscriber(queue)
        def subscriber(m) -> None:
            event.set()

        async with self.patch_broker(broker, with_real=True) as br:
            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("hello", queue)),
                    asyncio.create_task(event.wait()),
                ),
                timeout=3,
            )

        assert event.is_set()

    @pytest.mark.connected()
    async def test_inbox_prefix_with_real(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(inbox_prefix="test")

        async with self.patch_broker(broker, with_real=True) as br:
            assert br._connection._inbox_prefix == b"test"
            assert "test" in str(br._connection.new_inbox())

    async def test_respect_middleware(self, queue: str) -> None:
        routes = []

        class Middleware(BaseMiddleware):
            async def on_receive(self) -> None:
                routes.append(None)
                return await super().on_receive()

        broker = self.get_broker(middlewares=(Middleware,))

        @broker.subscriber(queue)
        async def h1(m) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m) -> None: ...

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

        @broker.subscriber(queue)
        async def h1(m) -> None: ...

        @broker.subscriber(queue + "1")
        async def h2(m) -> None: ...

        async with self.patch_broker(broker, with_real=True) as br:
            await br.publish("", queue)
            await br.publish("", queue + "1")
            await h1.wait_call(3)
            await h2.wait_call(3)

        assert len(routes) == 2

    async def test_js_subscriber_mock(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, stream=stream)
        async def m(msg) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue, stream=stream.name)
            m.mock.assert_called_once_with("hello")

    async def test_js_publisher_mock(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker()

        publisher = broker.publisher(queue + "resp")

        @publisher
        @broker.subscriber(queue, stream=stream)
        async def m(msg) -> str:
            return "response"

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue, stream=stream.name)
            publisher.mock.assert_called_with("response")

    async def test_any_subject_routing(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test.*.subj.*")
        def subscriber(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", "test.a.subj.b")
            subscriber.mock.assert_called_once_with("hello")

    async def test_ending_subject_routing(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test.>")
        def subscriber(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", "test.a.subj.b")
            subscriber.mock.assert_called_once_with("hello")

    async def test_mixed_subject_routing(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("*.*.subj.>")
        def subscriber(msg) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", "test.a.subj.b.c")
            subscriber.mock.assert_called_once_with("hello")

    async def test_consume_pull(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(queue, stream=stream, pull_sub=PullSub(1))
        def subscriber(m) -> None: ...

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            subscriber.mock.assert_called_once_with("hello")

    async def test_consume_batch(
        self,
        queue: str,
        stream: JStream,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            queue,
            stream=stream,
            pull_sub=PullSub(1, batch=True),
        )
        def subscriber(m) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            subscriber.mock.assert_called_once_with(["hello"])

    async def test_consume_with_subject_filter(self, queue: str) -> None:
        broker = self.get_broker()

        @broker.subscriber(
            config=ConsumerConfig(filter_subjects=[f"{queue}.a"]),
            stream=JStream(queue, subjects=[f"{queue}.*"]),
        )
        def subscriber(m) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish(1, f"{queue}.b")
            await br.publish(2, f"{queue}.a")
            subscriber.mock.assert_called_once_with(2)

    @pytest.mark.connected()
    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    @pytest.mark.connected()
    async def test_broker_with_real_doesnt_get_patched(self) -> None:
        await super().test_broker_with_real_doesnt_get_patched()

    @pytest.mark.connected()
    async def test_broker_with_real_patches_publishers_and_subscribers(
        self,
        queue: str,
    ) -> None:
        await super().test_broker_with_real_patches_publishers_and_subscribers(queue)

    @pytest.mark.xfail(reason="https://github.com/ag2ai/faststream/issues/2513")
    async def test_publisher_without_destination(self) -> None:
        """Fixes https://github.com/ag2ai/faststream/issues/2513."""
        broker = self.get_broker()

        # use two publishers to check that we don't have conflicts
        publisher = broker.publisher(subject="")
        another_publisher = broker.publisher(subject="")

        async with self.patch_broker(broker):
            await publisher.publish(None, subject="new-key")
            publisher.mock.assert_called_once()

            await another_publisher.publish(None, subject="new-key")
            another_publisher.mock.assert_called_once()
