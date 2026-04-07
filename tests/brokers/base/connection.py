from typing import Any

import pytest

from faststream._internal.broker import BrokerUsecase


class BrokerConnectionTestcase:
    broker: type[BrokerUsecase]

    def _get_broker_args(self, settings: Any) -> dict[str, Any]:
        return {}

    async def _ping(self, broker: BrokerUsecase) -> bool:
        return await broker.ping(timeout=5.0)

    @pytest.mark.asyncio()
    async def test_stop_before_start(self, settings: Any) -> None:
        kwargs = self._get_broker_args(settings)
        broker = self.broker(**kwargs)
        assert broker._connection is None
        await broker.stop()
        assert not broker.running

    @pytest.mark.asyncio()
    async def test_connect(self, settings: Any) -> None:
        kwargs = self._get_broker_args(settings)
        broker = self.broker(**kwargs)
        await broker.connect()
        assert await self._ping(broker)
        await broker.stop()
