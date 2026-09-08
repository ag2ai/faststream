from typing import Any

import pytest
from dirty_equals import IsPartialDict

from faststream.redis import StreamSub
from faststream.redis.annotations import RedisStreamMessage
from tests.marks import require_redis_v710

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig
from .stream_claim import StreamClaimTestcase


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestXReadGroupClaim(RedisTestcaseConfig, StreamClaimTestcase):
    pass


@pytest.mark.redis()
@pytest.mark.asyncio()
class TestXReadGroupClaimMemory(RedisMemoryTestcaseConfig):
    @require_redis_v710
    async def test_memory_broker_attaches_claim_metadata(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        raw: dict[str, Any] = {}

        @broker.subscriber(
            stream=StreamSub(
                queue,
                group="memory_claim_group",
                consumer="claimer",
                claim_min_idle_time=100,
            ),
        )
        async def handler(msg: str, message: RedisStreamMessage) -> None:
            raw.update(message.raw_message)

        async with self.patch_broker(broker) as br:
            await br.publish("hello", stream=queue)

        assert raw == IsPartialDict({"idle_times": [0], "delivery_counts": [0]})
