import pytest

from faststream.rabbit import Channel, RabbitBroker


@pytest.mark.asyncio
@pytest.mark.rabbit
async def test_set_max():
    broker = RabbitBroker(
        logger=None,
        default_channel=Channel(prefetch_count=10),
    )
    async with broker:
        await broker.start()
        assert broker._channel._prefetch_count == 10
