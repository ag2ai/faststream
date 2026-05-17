import pytest

from faststream.mq import MQBroker
from faststream.security import SASLPlaintext
from tests.brokers.base.connection import BrokerConnectionTestcase
from tests.marks import require_ibmmq


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
class TestConnection(BrokerConnectionTestcase):
    broker: type[MQBroker] = MQBroker

    def get_broker_args(self, settings):
        return {
            "queue_manager": settings.queue_manager,
            "channel": settings.channel,
            "conn_name": settings.conn_name,
            "username": settings.username,
            "password": settings.password,
        }

    @pytest.mark.asyncio()
    async def test_connect_handover_config_to_init(self, settings) -> None:
        broker = self.broker(
            queue_manager=settings.queue_manager,
            channel=settings.channel,
            host=settings.host,
            port=settings.port,
            security=SASLPlaintext(
                username=settings.username,
                password=settings.password,
            ),
        )
        assert await broker.connect()
        await broker.stop()
