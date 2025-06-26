import pytest

from faststream.rabbit import RabbitBroker, RabbitRouter


@pytest.fixture(params=(RabbitBroker(), RabbitRouter()))
def just_broker(request):
    return request.param


@pytest.mark.asyncio()  # run it async to create anyio.Event
async def test_sync(just_broker: RabbitBroker) -> None:
    @just_broker.subscriber("test")
    def func(a: int) -> str:
        return "pong"

    assert func(1) == "pong"


@pytest.mark.asyncio()  # run it async to create anyio.Event
async def test_sync_publisher(just_broker: RabbitBroker) -> None:
    @just_broker.publisher("test")
    def func(a: int) -> str:
        return "pong"

    assert func(1) == "pong"


@pytest.mark.asyncio()  # run it async to create anyio.Event
async def test_sync_multi(just_broker: RabbitBroker) -> None:
    @just_broker.publisher("test")
    @just_broker.subscriber("test")
    @just_broker.publisher("test")
    def func(a: int) -> str:
        return "pong"

    assert func(1) == "pong"


@pytest.mark.asyncio()
async def test_async(just_broker: RabbitBroker) -> None:
    @just_broker.subscriber("test")
    async def func(a: int) -> str:
        return "pong"

    assert await func(1) == "pong"


@pytest.mark.asyncio()
async def test_async_publisher(just_broker: RabbitBroker) -> None:
    @just_broker.publisher("test")
    async def func(a: int) -> str:
        return "pong"

    assert await func(1) == "pong"


@pytest.mark.asyncio()
async def test_async_multi(just_broker: RabbitBroker) -> None:
    @just_broker.publisher("test")
    @just_broker.subscriber("test")
    @just_broker.publisher("test")
    async def func(a: int) -> str:
        return "pong"

    assert await func(1) == "pong"
