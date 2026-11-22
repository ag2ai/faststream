from uuid import uuid4

import pytest

from faststream.rabbit import Channel, RabbitBroker


@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.rabbit()
async def test_subscriber_use_shared_channel() -> None:
    broker = RabbitBroker(logger=None)

    sub1 = broker.subscriber(uuid4().hex)
    sub2 = broker.subscriber(uuid4().hex, channel=Channel())

    shared_channel = Channel()
    sub3 = broker.subscriber(uuid4().hex, channel=shared_channel)
    sub4 = broker.subscriber(uuid4().hex, channel=shared_channel)

    async with broker:
        await broker.start()

        default_channel = broker._channel

        queue1, queue2, queue3, queue4 = (
            sub._queue_obj for sub in (sub1, sub2, sub3, sub4)
        )
        assert queue1
        assert queue2
        assert queue3
        assert queue4

        assert queue1.channel is default_channel

        assert queue2.channel is not default_channel

        assert queue3.channel is not default_channel
        assert queue3.channel is queue4.channel
