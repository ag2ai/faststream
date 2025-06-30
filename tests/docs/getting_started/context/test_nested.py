import pytest

from tests.marks import require_aiopika


@pytest.mark.asyncio()
@require_aiopika
async def test() -> None:
    from docs.docs_src.getting_started.context.nested import broker, handler
    from faststream.rabbit import TestRabbitBroker

    async with TestRabbitBroker(broker) as br:
        await br.publish("Hi!", "test")

        handler.mock.assert_called_once_with("Hi!")
