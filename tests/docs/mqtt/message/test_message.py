import pytest

from faststream.exceptions import SetupError
from faststream.mqtt import MQTTBroker, TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_message_fields_example() -> None:
    from docs.docs_src.mqtt.message.fields import broker, handle

    async with TestMQTTBroker(broker):
        await broker.publish(
            {"online": True},
            "devices/a/status",
            headers={"device": "a"},
        )
        handle.mock.assert_called_once()


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_path_example() -> None:
    from docs.docs_src.mqtt.message.path import broker, on_temperature

    async with TestMQTTBroker(broker):
        await broker.publish("22", "/devices/abc/temperature")
        on_temperature.mock.assert_called_once_with("22")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_literal_braces_example() -> None:
    from docs.docs_src.mqtt.message.literal_braces import (
        broker,
        handle,
        handle_status,
    )

    async with TestMQTTBroker(broker):
        await broker.publish("payload", "/root/{braced}")
        await broker.publish("ok", "/root/{braced}/status")
        handle.mock.assert_called_once_with("payload")
        handle_status.mock.assert_called_once_with("ok")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_wildcard_example() -> None:
    from docs.docs_src.mqtt.message.wildcard import broker, on_logs

    async with TestMQTTBroker(broker):
        await broker.publish("entry", "/devices/abc/logs/system/errors/critical")
        on_logs.mock.assert_called_once()


@pytest.mark.mqtt()
def test_invalid_template_example() -> None:
    broker = MQTTBroker("localhost", version="5.0")

    with pytest.raises(SetupError):
        broker.subscriber("/pre{name}/x")(lambda: None)
