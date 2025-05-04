import pytest

from faststream.redis import RedisBroker
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.redis()
class TestConnection(BrokerConnectionTestcase):
    broker = RedisBroker

    def get_broker_args(self, settings):
        return {
            "url": settings.url,
            "host": settings.host,
            "port": settings.port,
        }

    @pytest.mark.asyncio()
    async def test_init_connect_by_raw_data(self, settings) -> None:
        async with RedisBroker(
            "redis://localhost:6378",  # will be ignored
            host=settings.host,
            port=settings.port,
        ) as broker:
            assert await self.ping(broker)

    @pytest.mark.asyncio()
    async def test_connect_merge_kwargs_with_priority(self, settings) -> None:
        broker = self.broker(host="fake-host", port=6377)  # kwargs will be ignored
        await broker.connect(
            host=settings.host,
            port=settings.port,
        )
        assert await self.ping(broker)
        await broker.close()

    @pytest.mark.asyncio()
    async def test_connect_merge_args_and_kwargs_native(self, settings) -> None:
        broker = self.broker("fake-url")  # will be ignored
        await broker.connect(url=settings.url)
        assert await self.ping(broker)
        await broker.close()
