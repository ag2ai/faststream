from typing import Any

import pytest

from faststream.redis import RedisBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.connected()
@pytest.mark.redis()
class TestConnection(BrokerConnectionTestcase):
    broker = RedisBroker

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {
            "url": settings.url,
            "host": settings.host,
            "port": settings.port,
        }

    @pytest.mark.asyncio()
    async def test_init_connect_by_raw_data(self, settings: Settings) -> None:
        async with RedisBroker(
            "redis://localhost:63781",  # will be overridden by port and host options
            host=settings.host,
            port=settings.port,
        ) as broker:
            assert await self.ping(broker)
