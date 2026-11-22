import logging

import pytest

from docs.docs_src.nats.broker_events import app, broker, handle
from faststream.nats import TestApp, TestNatsBroker


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_broker_events_subscriber() -> None:
    async with TestNatsBroker(broker), TestApp(app):
        await broker.publish("Hello", "events.example")
        handle.mock.assert_called_once_with("Hello")


@pytest.mark.nats()
@pytest.mark.asyncio()
async def test_broker_events_callbacks(
    caplog: pytest.LogCaptureFixture,
) -> None:
    callbacks = broker._connection_kwargs

    with caplog.at_level(logging.INFO, logger="nats.events"):
        await callbacks["error_cb"](OSError("Connection refused"))
        await callbacks["disconnected_cb"]()
        await callbacks["closed_cb"]()
        await callbacks["discovered_server_cb"]()
        await callbacks["reconnected_cb"]()

    assert [
        (record.levelname, record.getMessage())
        for record in caplog.records
        if record.name == "nats.events"
    ] == [
        ("ERROR", "NATS client error: Connection refused"),
        ("WARNING", "Disconnected from NATS"),
        ("INFO", "NATS connection closed. Automatic reconnection has stopped"),
        ("INFO", "Discovered new NATS servers"),
        ("INFO", "Reconnected to NATS"),
    ]
