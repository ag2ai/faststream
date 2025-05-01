from uuid import uuid4

import pytest

from faststream.rabbit import Channel, RabbitBroker


@pytest.mark.asyncio
@pytest.mark.rabbit
async def test_subscriber_use_shared_channel():
    broker = RabbitBroker(logger=None)

    sub1 = broker.subscriber(uuid4().hex)
    sub2 = broker.subscriber(uuid4().hex, channel=Channel())

    shared_channel = Channel()
    sub3 = broker.subscriber(uuid4().hex, channel=shared_channel)
    sub4 = broker.subscriber(uuid4().hex, channel=shared_channel)

    async with broker:
        await broker.start()

        default_channel = broker._channel

        assert sub1._queue_obj.channel is default_channel

        assert sub2._queue_obj.channel is not default_channel

        assert sub3._queue_obj.channel is not default_channel
        assert sub3._queue_obj.channel is sub4._queue_obj.channel
