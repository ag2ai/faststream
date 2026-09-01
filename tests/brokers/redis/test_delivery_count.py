import asyncio

import pytest

from faststream.redis import Redis, RedisStreamMessage, StreamSub

from .basic import RedisTestcaseConfig


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestDeliveryCount(RedisTestcaseConfig):
    async def test_delivery_count_for_new_message(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        group = f"{queue}-workers"
        broker = self.get_broker(apply_types=True)
        counts: list[int] = []

        @broker.subscriber(stream=StreamSub(queue, group=group, consumer="worker-1"))
        async def handler(message: RedisStreamMessage, redis: Redis) -> None:
            counts.append(await message.get_delivery_count(redis, group))
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("hello", stream=queue)
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert counts == [1]

    async def test_delivery_count_after_xautoclaim_and_ack(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        group = f"{queue}-workers"
        broker = self.get_broker(apply_types=True)
        counts: list[int] = []

        @broker.subscriber(
            stream=StreamSub(
                queue,
                group=group,
                consumer="worker-2",
                min_idle_time=0,
            )
        )
        async def handler(message: RedisStreamMessage, redis: Redis) -> None:
            counts.append(await message.get_delivery_count(redis, group))
            await message.ack(redis=redis, group=group)
            counts.append(await message.get_delivery_count(redis, group))
            event.set()

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)
            await br._connection.xgroup_create(queue, group, id="0")
            await br._connection.xreadgroup(
                groupname=group,
                consumername="worker-1",
                streams={queue: ">"},
                count=1,
            )
            await br.start()
            await asyncio.wait_for(event.wait(), timeout=self.timeout)

        assert counts == [2, 1]
