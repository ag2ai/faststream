import pytest
import pytest_asyncio
from fastapi.exceptions import RequestValidationError

from faststream.rabbit import TestRabbitBroker

from .app import handler, publisher, router


@pytest_asyncio.fixture
async def broker():
    async with TestRabbitBroker(router.broker) as br:
        yield br


@pytest.mark.asyncio()
async def test_incorrect(broker):
    with pytest.raises(RequestValidationError):
        await broker.publish("user-id", "test-q")


@pytest.mark.asyncio()
async def test_handler(broker):
    user_id = 1

    await broker.publish(user_id, "test-q")

    handler.mock.assert_called_once_with(user_id)
    publisher.mock.assert_called_once_with(f"{user_id} created")
