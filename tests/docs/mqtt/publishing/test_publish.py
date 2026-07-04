import pytest

from faststream.exceptions import FeatureNotSupportedException
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_publish_example() -> None:
    from docs.docs_src.mqtt.publishing.publish import (
        broker,
        handle_alert,
        send_alert,
    )

    async with TestMQTTBroker(broker):
        await send_alert()
        handle_alert.mock.assert_called_once_with({"level": "warning"})


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_publisher_object_example() -> None:
    from docs.docs_src.mqtt.publishing.publisher_object import (
        broker,
        handle_command,
        handle_event,
    )

    async with TestMQTTBroker(broker):
        await broker.publish("reboot", "devices/commands")
        handle_command.mock.assert_called_once_with("reboot")
        handle_event.mock.assert_called_once_with({"command": "reboot"})


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_publisher_decorator_example() -> None:
    from docs.docs_src.mqtt.publishing.publisher_decorator import (
        broker,
        consume_processed,
        normalize,
    )

    async with TestMQTTBroker(broker):
        await broker.publish("done", "raw")
        normalize.mock.assert_called_once_with("done")
        consume_processed.mock.assert_called_once_with("DONE")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_mqtt311_rejects_headers() -> None:
    from faststream.mqtt import MQTTBroker

    broker = MQTTBroker("localhost", version="3.1.1")

    with pytest.raises(FeatureNotSupportedException, match="headers"):
        await broker.publish("msg", "topic", headers={"source": "docs"})
