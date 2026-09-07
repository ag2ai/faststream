import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from faststream.redis import RedisBroker, StreamClaimUnsupportedError, StreamSub
from faststream.redis.annotations import RedisBatchStreamMessage, RedisStreamMessage
from tests.marks import require_redis_v710

from .basic import RedisMemoryTestcaseConfig, RedisTestcaseConfig


async def skip_without_claim_support(broker: RedisBroker) -> None:
    """XREADGROUP CLAIM needs Redis server 8.4+; there is no client-side gate for it."""
    info = await broker._connection.info("server")
    major, minor, *_ = info["redis_version"].split(".")
    if (int(major), int(minor)) < (8, 4):
        pytest.skip("XREADGROUP CLAIM requires Redis server 8.4+")


async def make_pending(
    br: RedisBroker,
    queue: str,
    group: str,
    payloads: tuple[str, ...] = ("pending_message",),
) -> None:
    """Publish and read as the `temp` consumer, leaving the entries pending."""
    for payload in payloads:
        await br.publish(payload, stream=queue)

    with suppress(Exception):
        await br._connection.xgroup_create(queue, group, id="0", mkstream=True)

    await br._connection.xreadgroup(
        groupname=group,
        consumername="temp",
        streams={queue: ">"},
        count=len(payloads) + 10,
    )


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestXReadGroupClaim(RedisTestcaseConfig):
    @pytest.mark.slow()
    @require_redis_v710
    async def test_consume_claimed_and_new_in_one_handler(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        received: list[tuple[Any, int, int]] = []

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="claim_group",
                consumer="claimer",
                claim_min_idle_time=100,
            ),
        )
        async def handler(msg: str, message: RedisStreamMessage) -> None:
            received.append((
                msg,
                message.raw_message["delivery_counts"][0],
                message.raw_message["idle_times"][0],
            ))
            if len(received) >= 2:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await skip_without_claim_support(br)

            await make_pending(br, queue, "claim_group")
            await asyncio.sleep(0.3)
            await br.publish("new_message", stream=queue)

            await br.start()

            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

        assert event.is_set()
        # Claimed entries are reported before incoming ones
        assert received[0][0] == "pending_message"
        assert received[0][1] >= 1, "claimed entry counts previous deliveries"
        assert received[0][2] >= 100, "claimed entry was idle at least the threshold"
        assert received[1][0] == "new_message"
        assert received[1][1] == 0, "new entry has no previous deliveries"

    @pytest.mark.slow()
    @require_redis_v710
    async def test_batch_metadata_aligned(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        # Every delivery is kept: batches may split, and the in-flight batch
        # can be re-claimed before shutdown.
        snapshots: list[dict[str, Any]] = []

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="batch_claim_group",
                consumer="claimer",
                batch=True,
                claim_min_idle_time=300,
            ),
        )
        async def handler(msg: list, message: RedisBatchStreamMessage) -> None:
            snapshots.append(dict(message.raw_message))
            if sum(len(s["message_ids"]) for s in snapshots) >= 2:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await skip_without_claim_support(br)

            await make_pending(
                br,
                queue,
                "batch_claim_group",
                payloads=("first", "second"),
            )
            await asyncio.sleep(0.5)

            await br.start()

            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

        assert event.is_set()

        # Alignment must hold for every delivery our batch loop builds
        for snap in snapshots:
            assert (
                len(snap["message_ids"])
                == len(snap["delivery_counts"])
                == len(snap["idle_times"])
            )

        # Values are only predictable for the first delivery of each entry,
        # so stop scoring once the two original entries are covered.
        counts: list[int] = []
        idles: list[int] = []
        for snap in snapshots:
            counts.extend(snap["delivery_counts"])
            idles.extend(snap["idle_times"])
            if len(counts) >= 2:
                break

        assert counts == [1, 1]
        assert len(idles) == 2

    @pytest.mark.slow()
    @require_redis_v710
    async def test_repeated_get_one_keeps_claiming(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await make_pending(
                br,
                queue,
                "repeat_claim_group",
                payloads=("first", "second"),
            )
            await asyncio.sleep(0.3)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="repeat_claim_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            first = await subscriber.get_one(timeout=3)
            second = await subscriber.get_one(timeout=3)

            assert first is not None
            assert second is not None
            assert {await first.decode(), await second.decode()} == {
                "first",
                "second",
            }
            for message in (first, second):
                assert message.raw_message["delivery_counts"][0] >= 1
                assert message.raw_message["idle_times"][0] >= 100

            # The group read cursor survived both reads
            assert subscriber.read_id == ">"

    @pytest.mark.slow()
    @require_redis_v710
    async def test_iterator_repeated_messages(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await make_pending(
                br,
                queue,
                "iter_repeat_group",
                payloads=("first", "second"),
            )
            await asyncio.sleep(0.3)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="iter_repeat_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            got: set[str] = set()
            async for message in subscriber:
                got.add(await message.decode())
                assert message.raw_message["delivery_counts"][0] >= 1
                if len(got) >= 2:
                    break

            assert got == {"first", "second"}
            assert subscriber.read_id == ">"

    @pytest.mark.slow()
    @require_redis_v710
    async def test_unsupported_server_stops_subscriber(
        self,
        queue: str,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="unsupported_group",
                consumer="claimer",
                claim_min_idle_time=100,
            ),
        )
        async def handler(msg: str) -> None: ...

        async with self.patch_broker(consume_broker) as br:
            reject = AsyncMock(side_effect=ResponseError("syntax error"))
            with patch.object(Redis, "xreadgroup", reject):
                await br.start()
                await asyncio.sleep(0.3)

                calls_after_stop = reject.call_count
                await asyncio.sleep(0.3)
                # Stopped after the rejection instead of retrying in a hot loop
                assert reject.call_count == calls_after_stop

                tasks = br.subscribers[0].tasks
                found = False
                for t in tasks:
                    if not t.done():
                        continue
                    with suppress(asyncio.CancelledError, asyncio.InvalidStateError):
                        if isinstance(t.exception(), StreamClaimUnsupportedError):
                            found = True
                assert found, "Expected StreamClaimUnsupportedError to stop the task"

    @pytest.mark.slow()
    @require_redis_v710
    async def test_concurrent_subscriber(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        consume_broker = self.get_broker(apply_types=True)

        received: list[Any] = []

        @consume_broker.subscriber(
            stream=StreamSub(
                queue,
                group="concurrent_claim_group",
                consumer="claimer",
                claim_min_idle_time=100,
            ),
            max_workers=2,
        )
        async def handler(msg: str) -> None:
            received.append(msg)
            if len(received) >= 2:
                event.set()

        async with self.patch_broker(consume_broker) as br:
            await skip_without_claim_support(br)

            await make_pending(br, queue, "concurrent_claim_group")
            await asyncio.sleep(0.3)
            await br.publish("new_message", stream=queue)

            await br.start()

            await asyncio.wait(
                (asyncio.create_task(event.wait()),),
                timeout=3,
            )

        assert event.is_set()
        assert set(received) == {"pending_message", "new_message"}

    async def test_no_metadata_without_option(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)
        subscriber = broker.subscriber(
            stream=StreamSub(queue, group="plain_group", consumer="plain"),
        )

        async with self.patch_broker(broker) as br:
            await br.start()

            await br.publish("plain_message", stream=queue)

            message = await subscriber.get_one(timeout=3)

            assert message is not None
            assert "idle_times" not in message.raw_message
            assert "delivery_counts" not in message.raw_message


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

        assert raw["idle_times"] == [0]
        assert raw["delivery_counts"] == [0]
