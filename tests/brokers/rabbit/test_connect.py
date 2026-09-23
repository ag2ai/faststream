from typing import Any

import pytest

from faststream.rabbit import RabbitBroker
from faststream.security import SASLPlaintext
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.connected()
@pytest.mark.rabbit()
class TestConnection(BrokerConnectionTestcase):
    broker: type[RabbitBroker] = RabbitBroker

    def get_broker_args(self, settings: Any) -> Any:
        return {"url": settings.url}

    @pytest.mark.asyncio()
    async def test_connect_handover_config_to_init(
        self,
        settings: Any,
    ) -> None:
        broker = self.broker(
            host=settings.host,
            port=settings.port,
            security=SASLPlaintext(
                username=settings.login,
                password=settings.password,
            ),
        )
        connection = await broker.connect()
        assert not connection.is_closed
        await broker.stop()
