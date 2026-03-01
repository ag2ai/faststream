import pytest

from faststream.nats import TestNatsBroker, fastapi

router = fastapi.NatsRouter()


@router.subscriber("test")
async def handler(msg: str):
    ...


@pytest.mark.asyncio
async def test_router():
    async with TestNatsBroker(router.broker) as br:
        await br.publish("Hi!", "test")

        handler.mock.assert_called_once_with("Hi!")
