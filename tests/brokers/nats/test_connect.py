from typing import Any
from unittest.mock import MagicMock

import pytest
from nats.errors import Error

from faststream.nats import NatsBroker
from tests.brokers.base.connection import BrokerConnectionTestcase

from .settings import Settings


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


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_initial_authorization_violation_fails_fast() -> None:

    broker = NatsBroker()
    error_cb = broker._connection_kwargs["error_cb"]

    with pytest.raises(Error):
        await error_cb(Error("nats: 'Authorization Violation'"))
