import pytest

from faststream.redis import TestRedisBroker, fastapi

router = fastapi.RedisRouter()


@router.subscriber("test")
async def handler(msg: str):
    ...


@pytest.mark.asyncio
async def test_router():
    async with TestRedisBroker(router.broker) as br:
        await br.publish("Hi!", "test")

        handler.mock.assert_called_once_with("Hi!")
