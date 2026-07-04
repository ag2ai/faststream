import pytest

from faststream.redis import ListSub, StreamSub
from faststream.redis.testing import FakeProducer
from tests.brokers.base.testclient import BrokerTestclientTestcase

from .basic import RedisClusterMemoryTestcaseConfig


@pytest.mark.redis_cluster()
@pytest.mark.asyncio()
class TestClusterTestClient(RedisClusterMemoryTestcaseConfig, BrokerTestclientTestcase):
    """TestClient tests for RedisClusterBroker (memory-based)."""

    async def test_broker_gets_patched_attrs_within_cm(self) -> None:
        await super().test_broker_gets_patched_attrs_within_cm(FakeProducer)

    async def test_pub_sub_pattern(self) -> None:
        broker = self.get_broker()

        @broker.subscriber("test.{name}")
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, "test.name.useless")).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_list(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=queue)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, list=queue)).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(_) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", list=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_pub_by_pub_batch(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(list=ListSub(queue, batch=True))
        async def m(_) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish_batch("hello", list=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_batch_publisher_mock(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_list = ListSub(queue + "1", batch=True)
        publisher = broker.publisher(list=batch_list)

        @publisher
        @broker.subscriber(channel=queue)
        async def m(_):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_stream(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(stream=queue)
        async def handler(msg):
            return msg

        async with self.patch_broker(broker) as br:
            assert await (await br.request(1, stream=queue)).decode() == 1
            handler.mock.assert_called_once_with(1)

    async def test_stream_batch_pub_by_default_pub(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        @broker.subscriber(stream=StreamSub(queue, batch=True))
        async def m(_) -> None:
            pass

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)
            m.mock.assert_called_once_with(["hello"])

    async def test_stream_publisher(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker()

        batch_stream = StreamSub(queue + "1")
        publisher = broker.publisher(stream=batch_stream)

        @publisher
        @broker.subscriber(channel=queue)
        async def m(_):
            return 1, 2, 3

        async with self.patch_broker(broker) as br:
            await br.publish("hello", queue)
            m.mock.assert_called_once_with("hello")
            publisher.mock.assert_called_once_with([1, 2, 3])

    async def test_publish_to_none(self) -> None:
        broker = self.get_broker()

        async with self.patch_broker(broker) as br:
            with pytest.raises(ValueError):  # noqa: PT011
                await br.publish("hello")

    @pytest.mark.connected()
    async def test_with_real_testclient(
        self,
        queue: str,
    ) -> None:
        pytest.skip("Real-cluster testclient requires running cluster")

    @pytest.mark.connected()
    async def test_real_respect_middleware(self, queue: str) -> None:
        pytest.skip("Real-cluster middleware test requires running cluster")
