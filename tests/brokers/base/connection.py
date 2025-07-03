from typing import Type

import pytest

from faststream.broker.core.usecase import BrokerUsecase


class BrokerConnectionTestcase:
    broker: Type[BrokerUsecase]

    def get_broker_args(self, settings):
        return {}

    @pytest.mark.asyncio
    async def ping(self, broker) -> bool:
        return await broker.ping(timeout=5.0)

    @pytest.mark.asyncio
    async def test_close_before_start(self, async_mock):
        br = self.broker()
        assert br._connection is None
        await br.stop()
        br._connection = async_mock
        await br._close()
        assert not br.running

    @pytest.mark.asyncio
    async def test_init_connect_by_url(self, settings):
        kwargs = self.get_broker_args(settings)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self.ping(broker)
        await broker.stop()

    @pytest.mark.asyncio
    async def test_connection_by_url(self, settings):
        kwargs = self.get_broker_args(settings)
        broker = self.broker()
        await broker.connect(**kwargs)
        assert await self.ping(broker)
        await broker.stop()

    @pytest.mark.asyncio
    async def test_connect_by_url_priority(self, settings):
        kwargs = self.get_broker_args(settings)
        broker = self.broker("wrong_url")
        await broker.connect(**kwargs)
        assert await self.ping(broker)
        await broker.stop()

    @pytest.mark.asyncio
    async def test_ping_timeout(self, settings):
        kwargs = self.get_broker_args(settings)
        broker = self.broker("wrong_url")
        await broker.connect(**kwargs)
        assert not await broker.ping(timeout=1e-24)
        await broker.stop()
