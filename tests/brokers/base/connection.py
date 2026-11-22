from typing import Any

import pytest


class BrokerConnectionTestcase:
    broker: Any

    def get_broker_args(self, settings: Any) -> dict[str, Any]:
        return {}

    @pytest.mark.asyncio()
    async def ping(self, broker: Any) -> bool:
        is_alive: bool = await broker.ping(timeout=5.0)
        return is_alive

    @pytest.mark.asyncio()
    async def test_stop_before_start(self) -> None:
        br = self.broker()
        assert br._connection is None
        await br.stop()
        assert not br.running

    @pytest.mark.asyncio()
    async def test_connect(self, settings: Any) -> None:
        kwargs = self.get_broker_args(settings)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self.ping(broker)
        await broker.stop()
