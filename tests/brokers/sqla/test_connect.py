from typing import Any

import pytest

from faststream.sqla.broker.broker import SqlaBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .conftest import Settings


@pytest.mark.sqla()
@pytest.mark.connected()
class TestConnection(BrokerConnectionTestcase):
    broker = SqlaBroker

    def _get_broker_args(self, settings: Settings) -> dict[str, Any]:
        return {"engine": settings.engine}
