import asyncio
from contextlib import suppress
from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.asyncio import Redis
from redis.exceptions import ResponseError

from faststream.redis import (
    RedisBroker,
    StreamClaimUnsupportedError,
    StreamSub,
    TestRedisBroker,
)
from faststream.redis._compat import REDIS_V710
from faststream.redis.annotations import RedisBatchStreamMessage, RedisStreamMessage

from .basic import RedisTestcaseConfig

pytestmark = pytest.mark.skipif(
    not REDIS_V710,
    reason="`claim_min_idle_time` requires redis-py 7.1.0+",
)


async def skip_without_claim_support(broker: RedisBroker) -> None:
    """XREADGROUP CLAIM requires Redis server 8.4+ - skip on older servers.

    There is no repo-wide precedent for server-version gating yet; this is
    modeled on the capability-probe skips used by the cluster tests.
    """
    info = await broker._connection.info("server")
    major, minor, *_ = info["redis_version"].split(".")
    if (int(major), int(minor)) < (8, 4):
        pytest.skip("XREADGROUP CLAIM requires Redis server 8.4+")


@pytest.mark.connected()
@pytest.mark.redis()
@pytest.mark.asyncio()
class TestXReadGroupClaim(RedisTestcaseConfig):
    async def _make_pending(
        self,
        br: RedisBroker,
        queue: str,
        group: str,
        payloads: tuple[str, ...] = ("pending_message",),
    ) -> None:
        """Publish messages and leave them pending for the `temp` consumer."""
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

    @pytest.mark.slow()
    async def test_consume_claimed_and_new_in_one_handler(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """A single subscriber reclaims pending messages and consumes new ones."""
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

            await self._make_pending(br, queue, "claim_group")
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
    async def test_batch_metadata_aligned(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """Batch messages carry per-entry metadata aligned with `message_ids`."""
        consume_broker = self.get_broker(apply_types=True)

        # Deliveries are accumulated (never overwritten) so the scoring below
        # does not depend on delivery timing (split batches, re-claims of the
        # in-flight batch before shutdown).
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

            await self._make_pending(
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

        # Value checks only hold for the first delivery of each entry, so
        # score the deliveries covering the two original entries and ignore
        # any later re-claims
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
    async def test_get_one_with_claim(self, queue: str) -> None:
        """`get_one()` exposes the claim metadata of a reclaimed message."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await self._make_pending(br, queue, "get_one_claim_group")
            await asyncio.sleep(0.3)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="get_one_claim_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            message = await subscriber.get_one(timeout=3)

            assert message is not None
            assert await message.decode() == "pending_message"
            assert message.raw_message["delivery_counts"][0] >= 1
            assert message.raw_message["idle_times"][0] >= 100

    @pytest.mark.slow()
    async def test_iterator_with_claim(self, queue: str) -> None:
        """The async iterator exposes the claim metadata of a reclaimed message."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await self._make_pending(br, queue, "iter_claim_group")
            await asyncio.sleep(0.3)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="iter_claim_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            async for message in subscriber:
                assert await message.decode() == "pending_message"
                assert message.raw_message["delivery_counts"][0] >= 1
                break

    @pytest.mark.slow()
    async def test_repeated_get_one_keeps_claiming(self, queue: str) -> None:
        """Repeated `get_one()` calls keep the `>` cursor and keep claiming."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await self._make_pending(
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

            # The group read cursor survived both reads
            assert subscriber.read_id == ">"

    @pytest.mark.slow()
    async def test_iterator_repeated_messages(self, queue: str) -> None:
        """The async iterator keeps the `>` cursor across messages."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await self._make_pending(
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
    async def test_zero_claim_min_idle_time(self, queue: str) -> None:
        """`claim_min_idle_time=0` is a valid value claiming without idle wait."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            await self._make_pending(br, queue, "zero_claim_group")

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="zero_claim_group",
                    consumer="claimer",
                    claim_min_idle_time=0,
                ),
            )

            message = await subscriber.get_one(timeout=3)

            assert message is not None
            assert message.raw_message["delivery_counts"][0] >= 1

    @pytest.mark.slow()
    async def test_unsupported_server_stops_subscriber(
        self,
        queue: str,
    ) -> None:
        """A pre-8.4 server rejecting CLAIM stops the subscriber - no hot loop."""
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
                # The subscriber stopped after the rejection instead of
                # retrying in a hot loop
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
    async def test_unsupported_server_maps_error_in_get_one(self, queue: str) -> None:
        """`get_one()` maps a CLAIM rejection to `StreamClaimUnsupportedError`."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="unsupported_get_one_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            reject = AsyncMock(side_effect=ResponseError("syntax error"))
            with (
                patch.object(Redis, "xreadgroup", reject),
                pytest.raises(StreamClaimUnsupportedError),
            ):
                await subscriber.get_one(timeout=1)

    @pytest.mark.slow()
    async def test_unsupported_server_maps_error_in_iterator(self, queue: str) -> None:
        """The async iterator maps a CLAIM rejection to `StreamClaimUnsupportedError`."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="unsupported_iter_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            reject = AsyncMock(side_effect=ResponseError("syntax error"))
            with patch.object(Redis, "xreadgroup", reject):
                iterator = subscriber.__aiter__()
                with pytest.raises(StreamClaimUnsupportedError):
                    await anext(iterator)

    @pytest.mark.slow()
    async def test_tombstone_is_not_claimed(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """Deleted pending entries are not claimed and stay in the PEL."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await skip_without_claim_support(br)
            await br.start()

            msg_id = await br.publish("will_delete", stream=queue)

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue,
                    "tombstone_claim_group",
                    id="0",
                    mkstream=True,
                )
            await br._connection.xreadgroup(
                groupname="tombstone_claim_group",
                consumername="temp",
                streams={queue: ">"},
                count=10,
            )
            await br._connection.xdel(queue, msg_id)
            await asyncio.sleep(0.3)

            subscriber = br.subscriber(
                stream=StreamSub(
                    queue,
                    group="tombstone_claim_group",
                    consumer="claimer",
                    claim_min_idle_time=100,
                ),
            )

            mock(await subscriber.get_one(timeout=0.5))

            mock.assert_called_once_with(None)
            pending = await br._connection.xpending(queue, "tombstone_claim_group")
            assert pending["pending"] == 1, "tombstone stays in the PEL"

    @pytest.mark.slow()
    async def test_concurrent_subscriber(
        self,
        queue: str,
        event: asyncio.Event,
    ) -> None:
        """`max_workers` and `claim_min_idle_time` work together."""
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

            await self._make_pending(br, queue, "concurrent_claim_group")
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
        """Regression: without the option the raw message stays unchanged."""
        broker = self.get_broker(apply_types=True)

        async with self.patch_broker(broker) as br:
            await br.start()

            with suppress(Exception):
                await br._connection.xgroup_create(
                    queue,
                    "plain_group",
                    id="0",
                    mkstream=True,
                )

            subscriber = br.subscriber(
                stream=StreamSub(queue, group="plain_group", consumer="plain"),
            )

            await br.publish("plain_message", stream=queue)

            message = await subscriber.get_one(timeout=3)

            assert message is not None
            assert "idle_times" not in message.raw_message
            assert "delivery_counts" not in message.raw_message


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_memory_broker_attaches_claim_metadata(queue: str) -> None:
    """TestRedisBroker exposes the metadata keys the docs reference."""
    broker = RedisBroker(apply_types=True)

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

    async with TestRedisBroker(broker) as br:
        await br.publish("hello", stream=queue)

    assert raw["idle_times"] == [0]
    assert raw["delivery_counts"] == [0]
