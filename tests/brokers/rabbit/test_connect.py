from typing import Type

import pytest

from faststream.rabbit import RabbitBroker
from faststream.security import SASLPlaintext
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.rabbit
class TestConnection(BrokerConnectionTestcase):
    broker: Type[RabbitBroker] = RabbitBroker

    def get_broker_args(self, settings):
        return {"url": settings.url}

    @pytest.mark.asyncio
    async def test_connect_handover_config_to_init(self, settings):
        broker = self.broker(
            host=settings.host,
            port=settings.port,
            security=SASLPlaintext(
                username=settings.login,
                password=settings.password,
            ),
        )
        assert await broker.connect()
        await broker.stop()

    @pytest.mark.asyncio
    async def test_connect_handover_config_to_connect(self, settings):
        broker = self.broker()
        assert await broker.connect(
            host=settings.host,
            port=settings.port,
            security=SASLPlaintext(
                username=settings.login,
                password=settings.password,
            ),
        )
        await broker.stop()

    @pytest.mark.asyncio
    async def test_connect_handover_config_to_connect_override_init(self, settings):
        broker = self.broker("fake-url")  # will be ignored
        assert await broker.connect(url=settings.url)
        await broker.stop()
