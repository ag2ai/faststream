from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase


class BrokerConnectionTestcase:
    broker: type[BrokerUsecase]

    def get_broker_args(self, settings: Any) -> dict[str, Any]:
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
    async def test_connect(self, settings: Any) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self.ping(broker)
        await broker.close()
