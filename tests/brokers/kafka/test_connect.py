from typing import Any

import pytest

from faststream.kafka import KafkaBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.kafka()
class TestConnection(BrokerConnectionTestcase):
    broker = KafkaBroker

    def get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {"bootstrap_servers": settings.url}
