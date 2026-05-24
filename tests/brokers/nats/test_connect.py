from typing import Any
from unittest.mock import MagicMock

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

    def test_js_options(self, mock: MagicMock) -> None:
        broker = NatsBroker(js_options={"prefix": "test"})
        broker.config.connect(mock)
        mock.jetstream.assert_called_once_with(prefix="test")
