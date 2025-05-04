import pytest

from faststream._internal.broker.broker import BrokerUsecase


class BrokerConnectionTestcase:
    broker: type[BrokerUsecase]

    def get_broker_args(self, settings):
        return {}

    @pytest.mark.asyncio()
    async def ping(self, broker) -> bool:
        return await broker.ping(timeout=5.0)

    @pytest.mark.asyncio()
    async def test_close_before_start(self) -> None:
        br = self.broker()
        assert br._connection is None
        await br.close()
        assert not br.running

    @pytest.mark.asyncio()
    async def test_init_connect_by_url(self, settings) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self.ping(broker)
        await broker.close()

    @pytest.mark.asyncio()
    async def test_connection_by_url(self, settings) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker()
        await broker.connect(**kwargs)
        assert await self.ping(broker)
        await broker.close()

    @pytest.mark.asyncio()
    async def test_connect_by_url_priority(self, settings) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker("wrong_url")
        await broker.connect(**kwargs)
        assert await self.ping(broker)
        await broker.close()

    @pytest.mark.asyncio()
    async def test_ping_timeout(self, settings) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker("wrong_url")
        await broker.connect(**kwargs)
        assert not await broker.ping(timeout=1e-24)
        await broker.close()
