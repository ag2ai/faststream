from typing import Any

import aio_pika
import pytest

from faststream import ContextRepo, Depends
from faststream.rabbit import RabbitBroker
from faststream.rabbit.annotations import RabbitMessage


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_broker_depends(queue: str) -> None:
    full_broker = RabbitBroker(apply_types=True)

    def sync_depends(message: RabbitMessage) -> Any:
        return message

    async def async_depends(message: RabbitMessage) -> Any:
        return message

    check_message = None

    @full_broker.subscriber(queue)
    async def h(
        message: RabbitMessage,
        k1: Any = Depends(sync_depends),
        k2: Any = Depends(async_depends),
    ) -> None:
        nonlocal check_message
        check_message = (message is k1) and (message is k2)

    await full_broker.start()

    await full_broker.request(queue=queue)
    assert check_message is True


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_different_consumers_has_different_messages(
    context: ContextRepo,
) -> None:
    full_broker = RabbitBroker(apply_types=True)

    message1 = None

    @full_broker.subscriber("test_different_consume_1")
    async def consumer1(message: RabbitMessage) -> None:
        nonlocal message1
        message1 = message

    message2 = None

    @full_broker.subscriber("test_different_consume_2")
    async def consumer2(message: RabbitMessage) -> None:
        nonlocal message2
        message2 = message

    await full_broker.start()

    await full_broker.request(queue="test_different_consume_1")
    await full_broker.request(queue="test_different_consume_2")

    assert message1
    assert message2
    assert isinstance(message1.raw_message, aio_pika.IncomingMessage)
    assert isinstance(message2.raw_message, aio_pika.IncomingMessage)
    assert message1 != message2
    assert context.message is None
