from unittest.mock import patch

import pytest

from faststream.exceptions import NackMessage
from faststream.redis import RedisBroker, StreamSub
from faststream.redis.testing import PEL, TestRedisBroker


@pytest.fixture()
def broker() -> RedisBroker:
    return RedisBroker()


@pytest.fixture()
def pel() -> PEL:
    return PEL()


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_different_groups_do_not_interfere(
    broker: RedisBroker,
    pel: PEL,
) -> None:
    @broker.subscriber(stream=StreamSub("tasks", group="group-a", consumer="a1"))
    async def worker_a(msg: str) -> None:
        raise NackMessage

    @broker.subscriber(stream=StreamSub("tasks", group="group-b", consumer="b1"))
    async def worker_b(msg: str) -> None: ...

    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")
        assert len(pel.entries) == 1


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_no_ack_policy_skips_pel_tracking(
    broker: RedisBroker,
    pel: PEL,
) -> None:
    @broker.subscriber(stream=StreamSub("tasks", no_ack=True))
    async def worker(msg: str) -> None: ...

    async with TestRedisBroker(broker, pel=pel) as br:
        with patch.object(pel, "put") as put_mock:
            await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        put_mock.assert_not_called()
        assert pel.entries == {}


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_no_ack_policy_skips_pel_tracking_on_failure(
    broker: RedisBroker,
    pel: PEL,
) -> None:
    @broker.subscriber(stream=StreamSub("tasks", no_ack=True))
    async def worker(msg: str) -> None:
        error_msg = "boom"
        raise ValueError(error_msg)

    async with TestRedisBroker(broker, pel=pel) as br:
        with (
            patch.object(pel, "put") as put_mock,
            pytest.raises(ValueError, match="boom"),
        ):
            await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        put_mock.assert_not_called()
        assert pel.entries == {}


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_only_min_idle_time_subscriber_processes_pel(
    pel: PEL,
) -> None:
    call_order: list[str] = []
    while "worker" not in call_order:
        broker = RedisBroker()
        call_order = []

        @broker.subscriber(stream=StreamSub("tasks", group="workers", consumer="w1"))
        async def worker(msg: str) -> None:
            call_order.append("worker")  # noqa: B023
            raise NackMessage

        @broker.subscriber(
            stream=StreamSub(
                "tasks",
                group="workers",
                consumer="claimer",
                min_idle_time=10000,
            ),
        )
        async def claimer(msg: str) -> None:
            call_order.append("claimer")  # noqa: B023

        pel = PEL()
        async with TestRedisBroker(broker, pel=pel) as br:
            await br.publish("data", stream="tasks")
    assert len(pel.entries) == 0


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pel_cleared_after_claimer_processes_it(
    broker: RedisBroker,
    pel: PEL,
) -> None:
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

    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")

        claimer.mock.assert_called_once_with("data")
        assert pel.entries == {}


@pytest.mark.redis()
@pytest.mark.asyncio()
async def test_pel_not_cleared_without_a_claimer(
    broker: RedisBroker,
    pel: PEL,
) -> None:
    @broker.subscriber(stream=StreamSub("tasks", group="workers", consumer="w1"))
    async def worker(msg: str) -> None:
        raise NackMessage

    async with TestRedisBroker(broker, pel=pel) as br:
        await br.publish("data", stream="tasks")

        worker.mock.assert_called_once_with("data")
        assert len(pel.entries) == 1
