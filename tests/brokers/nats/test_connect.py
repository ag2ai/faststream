import pytest

from faststream.nats import NatsBroker
from tests.brokers.base.connection import BrokerConnectionTestcase


@pytest.mark.nats()
class TestConnection(BrokerConnectionTestcase):
    broker = NatsBroker

    def get_broker_args(self, settings):
        return {"servers": settings.url}
