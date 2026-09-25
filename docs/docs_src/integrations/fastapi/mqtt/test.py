import pytest

from faststream.mqtt import TestMQTTBroker, fastapi

router = fastapi.MQTTRouter()


@router.subscriber("test")
async def handler(msg: str):
    ...


@pytest.mark.asyncio
async def test_router():
    async with TestMQTTBroker(router.broker) as br:
        await br.publish("Hi!", "test")

        handler.mock.assert_called_once_with("Hi!")
