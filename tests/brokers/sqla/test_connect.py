from typing import Any

import pytest

from faststream.sqla.broker.broker import SqlaBroker
from tests.brokers.base.connection import BrokerConnectionTestcase
from tests.brokers.sqla.helpers import Settings


@pytest.mark.sqla()
@pytest.mark.connected()
class TestConnection(BrokerConnectionTestcase):
    broker = SqlaBroker

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {"engine": settings.engine}

    @pytest.mark.asyncio()
    async def test_stop_before_start(self, settings: Settings) -> None:
        br = self.broker(**self.get_broker_args(settings))
        assert br._connection is None
        await br.stop()
        assert not br.running
