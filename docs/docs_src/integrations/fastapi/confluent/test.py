import pytest

from faststream.confluent import TestKafkaBroker, fastapi

router = fastapi.KafkaRouter()


@router.subscriber("test")
async def handler(msg: str):
    ...


@pytest.mark.asyncio
async def test_router():
    async with TestKafkaBroker(router.broker) as br:
        await br.publish("Hi!", "test")

        handler.mock.assert_called_once_with("Hi!")
