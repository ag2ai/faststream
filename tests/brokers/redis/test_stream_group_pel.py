from unittest.mock import patch

import pytest

from faststream.exceptions import NackMessage
from faststream.redis import RedisBroker, StreamSub
from faststream.redis.testing import PEL, TestRedisBroker


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_different_groups_do_not_interfere() -> None:
    broker = RedisBroker()

    @broker.subscriber(stream=StreamSub("tasks", group="group-a", consumer="a1"))
    async def worker_a(msg: str) -> None:
        raise NackMessage

    @broker.subscriber(stream=StreamSub("tasks", group="group-b", consumer="b1"))
    async def worker_b(msg: str) -> None: ...

    pel = PEL()
    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")

        worker_a.mock.assert_called_once_with("data")
        # group-b is unaffected by group-a's nack: it still gets delivered normally.
        worker_b.mock.assert_called_once_with("data")



@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_no_ack_policy_skips_pel_tracking() -> None:
    broker = RedisBroker()

    @broker.subscriber(stream=StreamSub("tasks", no_ack=True))
    async def worker(msg: str) -> None: ...

    pel = PEL()
    async with TestRedisBroker(broker, pel=pel) as br:
        with patch.object(pel, "put") as put_mock:
            await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        # no_ack consumers are never tracked in the PEL at all.
        put_mock.assert_not_called()
        assert pel._entries == {}


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_only_min_idle_time_subscriber_processes_pel() -> None:
    broker = RedisBroker()

    @broker.subscriber(stream=StreamSub("tasks", group="workers", consumer="w1"))
    async def worker(msg: str) -> None:
        raise NackMessage

    @broker.subscriber(
        stream=StreamSub(
            "tasks",
            group="workers",
            consumer="claimer",
            min_idle_time=10000,
        ),
    )
    async def claimer(msg: str) -> None: ...

    pel = PEL()
    async with TestRedisBroker(broker, pel=pel) as br:
        with patch.object(pel, "get_entry", wraps=pel.get_entry) as get_entry_mock:
            await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        # worker nacked, so the entry stayed in the PEL and was reclaimed
        claimer.mock.assert_called_once_with("data")
        # only the min_idle_time consumer ever looks at the PEL, not `worker`
        get_entry_mock.assert_called_once()


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pel_cleared_after_claimer_processes_it() -> None:
    broker = RedisBroker()

    @broker.subscriber(stream=StreamSub("tasks", group="workers", consumer="w1"))
    async def worker(msg: str) -> None:
        raise NackMessage

    @broker.subscriber(
        stream=StreamSub(
            "tasks",
            group="workers",
            consumer="claimer",
            min_idle_time=10000,
        ),
    )
    async def claimer(msg: str) -> None: ...

    pel = PEL()
    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")

        claimer.mock.assert_called_once_with("data")
        assert pel._entries == {}


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pel_not_cleared_without_a_claimer() -> None:
    broker = RedisBroker()

    @broker.subscriber(stream=StreamSub("tasks", group="workers", consumer="w1"))
    async def worker(msg: str) -> None:
        raise NackMessage

    pel = PEL()
    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        # nothing exists to reclaim it, so the pending entry just stays put.
        assert len(pel._entries) == 1
