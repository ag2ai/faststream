import pytest

from faststream.rabbit import RabbitBroker, RabbitExchange, RabbitQueue
from faststream.rabbit.utils import RabbitDeclarer


@pytest.mark.asyncio()
async def test_declare_queue(async_mock, queue: str):
    declarer = RabbitDeclarer(async_mock)

    q1 = await declarer.declare_queue(RabbitQueue(queue))
    q2 = await declarer.declare_queue(RabbitQueue(queue))

    assert q1 is q2
    async_mock.declare_queue.assert_awaited_once()


@pytest.mark.asyncio()
async def test_declare_exchange(
    async_mock,
    queue: str,
):
    declarer = RabbitDeclarer(async_mock)

    ex1 = await declarer.declare_exchange(RabbitExchange(queue))
    ex2 = await declarer.declare_exchange(RabbitExchange(queue))

    assert ex1 is ex2
    async_mock.declare_exchange.assert_awaited_once()


@pytest.mark.asyncio()
async def test_declare_nested_exchange_cash_nested(
    async_mock,
    queue: str,
):
    declarer = RabbitDeclarer(async_mock)

    exchange = RabbitExchange(queue)

    await declarer.declare_exchange(RabbitExchange(queue + "1", bind_to=exchange))
    assert async_mock.declare_exchange.await_count == 2

    await declarer.declare_exchange(exchange)
    assert async_mock.declare_exchange.await_count == 2


@pytest.mark.asyncio()
async def test_publisher_declare(
    async_mock,
    queue: str,
):
    declarer = RabbitDeclarer(async_mock)

    broker = RabbitBroker()
    broker._connection = async_mock
    broker.declarer = declarer

    @broker.publisher(queue, queue)
    async def f(): ...

    await broker.start()

    assert not async_mock.declare_queue.await_count
    async_mock.declare_exchange.assert_awaited_once()
