from typing import Any
from unittest.mock import MagicMock

import pytest
from nats.errors import Error

from faststream.nats import NatsBroker
from faststream.nats.broker.broker import UNRECOVERABLE_CONNECT_ERRORS
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
@pytest.mark.parametrize("reason", UNRECOVERABLE_CONNECT_ERRORS)
async def test_unrecoverable_connect_error_fails_fast(reason: str) -> None:
    broker = NatsBroker()
    error_cb = broker._connection_kwargs["error_cb"]

    with pytest.raises(Error):
        await error_cb(Error(f"nats: '{reason.title()}'"))


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_recoverable_connect_error_does_not_raise() -> None:
    broker = NatsBroker()
    error_cb = broker._connection_kwargs["error_cb"]

    await error_cb(Error("nats: 'Slow Consumer'"))
