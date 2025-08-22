from unittest.mock import patch

import pytest

from faststream import TestApp
from faststream.kafka import TestKafkaBroker
from faststream.nats import TestNatsBroker
from tests.marks import pydantic_v2
from tests.mocks import mock_pydantic_settings_env


@pydantic_v2
@pytest.mark.asyncio()
async def test_kafka_context_valid_broker():
    """Tests that the Kafka broker is initialized correctly with valid settings."""
    with mock_pydantic_settings_env({"any_flag": "True"}):
        from docs.docs_src.getting_started.cli.kafka_context import app, broker

        async with TestKafkaBroker(broker) as br, TestApp(app, {"env": ""}):
            assert br is not None
            assert app.context.get("settings").any_flag


@pydantic_v2
@pytest.mark.asyncio()
async def test_kafka_context_invalid_broker():
    """Tests how the Kafka broker handles invalid broker configurations. This test uses a mock to simulate a connection failure."""
    with mock_pydantic_settings_env({"url": "invalid_url:9092"}), patch(
        "faststream.kafka.broker.KafkaBroker.connect",
        side_effect=RuntimeError("mocked connection error"),
    ):
        from docs.docs_src.getting_started.cli.kafka_context import broker

        with pytest.raises(RuntimeError, match="mocked connection error"):
            async with TestKafkaBroker(broker):
                pass


@pydantic_v2
@pytest.mark.asyncio()
async def test_nats_context_valid_broker():
    """Tests that the NATS broker is initialized correctly with valid settings."""
    with mock_pydantic_settings_env({"any_flag": "True"}):
        from docs.docs_src.getting_started.cli.nats_context import app, broker

        async with TestNatsBroker(broker) as br, TestApp(app, {"env": ""}):
            assert br is not None
            assert app.context.get("settings").any_flag


@pydantic_v2
@pytest.mark.asyncio()
async def test_nats_context_invalid_broker():
    """Tests how the NATS broker handles invalid broker configurations. This test uses a mock to simulate a connection failure."""
    with mock_pydantic_settings_env({"url": "nats://invalid_url:4222"}), patch(
        "faststream.nats.broker.NatsBroker.connect",
        side_effect=RuntimeError("mocked connection error"),
    ):
        from docs.docs_src.getting_started.cli.nats_context import broker

        with pytest.raises(RuntimeError, match="mocked connection error"):
            async with TestNatsBroker(broker):
                pass
