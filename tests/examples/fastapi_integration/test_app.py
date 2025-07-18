import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test_handler() -> None:
    from examples.fastapi_integration.testing import (
        router,
        test_handler as test_,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(router.broker) as br:
        await test_(br)


@pytest.mark.asyncio()
@require_aiopika
async def test_incorrect() -> None:
    from examples.fastapi_integration.testing import (
        router,
        test_incorrect as test_,
    )
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(router.broker) as br:
        await test_(br)
