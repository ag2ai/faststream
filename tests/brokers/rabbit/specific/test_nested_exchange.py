from asyncio import Event, wait_for

import pytest

from faststream.rabbit import ExchangeType, RabbitBroker, RabbitExchange


@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_bind_to(queue: str) -> None:
    broker = RabbitBroker(apply_types=False)

    consume = Event()

    async with broker:
        meta_parent = RabbitExchange("meta", type=ExchangeType.FANOUT)
        parent_exch = RabbitExchange(
            "main",
            type=ExchangeType.FANOUT,
            bind_to=meta_parent,
        )

        @broker.subscriber(
            queue,
            exchange=RabbitExchange("nested", bind_to=parent_exch),
        )
        async def handler(m) -> None:
            consume.set()

        await broker.start()
        await broker.publish(message="hello", queue=queue, exchange=meta_parent)
        await wait_for(consume.wait(), 3)
