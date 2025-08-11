from typing import Any

import pytest

from faststream.nats import NatsBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.connected()
@pytest.mark.nats()
class TestConnection(BrokerConnectionTestcase):
    broker = NatsBroker

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {"servers": settings.url}
