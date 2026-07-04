import pytest

from faststream.exceptions import FeatureNotSupportedException
from faststream.mqtt import TestMQTTBroker


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_mqtt5_rpc_example() -> None:
    from docs.docs_src.mqtt.rpc.mqtt5 import broker, demo, echo

    async with TestMQTTBroker(broker):
        await demo()
        echo.mock.assert_called_once_with("hello")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_mqtt_response_example() -> None:
    from docs.docs_src.mqtt.rpc.mqtt_response import broker, status

    async with TestMQTTBroker(broker):
        reply = await broker.request("", "rpc/status")
        status.mock.assert_called_once_with("")

    assert reply.body == b'{"ok":true}'


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_mqtt311_rpc_example() -> None:
    from docs.docs_src.mqtt.rpc.mqtt311 import broker, demo, manual_reply_echo

    async with TestMQTTBroker(broker):
        await demo()
        manual_reply_echo.mock.assert_called_once_with("hello")


@pytest.mark.mqtt()
@pytest.mark.asyncio()
async def test_mqtt311_requires_reply_to() -> None:
    from faststream.mqtt import MQTTBroker

    broker = MQTTBroker("localhost", version="3.1.1")

    with pytest.raises(
        FeatureNotSupportedException,
        match="requires an explicit reply_to topic",
    ):
        await broker.request("hello", "rpc/manual-reply/echo")
