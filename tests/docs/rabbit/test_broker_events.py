import logging

import pytest

from docs.docs_src.rabbit.broker_events import (
    app,
    broker,
    handle,
    on_close,
    on_reconnected,
)
from faststream.rabbit import TestApp, TestRabbitBroker
from tests.marks import require_aiopika


@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_broker_events_example(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="rabbit.events"):
        await on_close(None, RuntimeError("Connection lost"))
        await on_reconnected(None)
        async with TestRabbitBroker(broker), TestApp(app):
            await broker.publish("hello", "events.example")
            handle.mock.assert_called_once_with("hello")

    assert "RabbitMQ connection closed: RuntimeError('Connection lost')" in caplog.text
    assert "RabbitMQ connection restored" in caplog.text


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
@require_aiopika
async def test_broker_events_example_shutdown(caplog: pytest.LogCaptureFixture) -> None:
    with caplog.at_level(logging.INFO, logger="rabbit.events"):
        async with broker:
            pass

    assert "RabbitMQ connection closed during shutdown" in caplog.text
