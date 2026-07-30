import pytest

from faststream.redis.testing import PEL, TestRedisBroker

from .pel_multiple_groups import billing_worker
from .pel_multiple_groups import broker as multiple_groups_broker
from .pel_multiple_groups import shipping_worker
from .pel_no_ack import broker as no_ack_broker
from .pel_no_ack import fire_and_forget_worker
from .pel_pending import broker as pending_broker
from .pel_pending import flaky_worker as pending_worker
from .pel_reprocessing import broker as reprocessing_broker
from .pel_reprocessing import claiming_worker
from .pel_reprocessing import flaky_worker as reprocessing_worker


@pytest.mark.asyncio
async def test_pending_message_stays_without_a_claimer() -> None:
    pel = PEL()

    async with TestRedisBroker(pending_broker, pel=pel) as br:
        await br.publish("order-1", stream="orders")

        pending_worker.mock.assert_called_once_with("order-1")
        # nothing exists to reclaim it, so the entry just stays pending
        assert len(pel._entries) == 1


@pytest.mark.asyncio
async def test_each_group_gets_its_own_pel_entry() -> None:
    pel = PEL()

    async with TestRedisBroker(multiple_groups_broker, pel=pel) as br:
        await br.publish("order-4", stream="orders")

        billing_worker.mock.assert_called_once_with("order-4")
        shipping_worker.mock.assert_called_once_with("order-4")
        # one entry per group, tracked independently for the same message
        assert len(pel._entries) == 2


@pytest.mark.asyncio
async def test_no_ack_worker_never_touches_the_pel() -> None:
    pel = PEL()

    async with TestRedisBroker(no_ack_broker, pel=pel) as br:
        with pytest.raises(ValueError, match="Could not process order"):
            await br.publish("order-3", stream="orders")

        fire_and_forget_worker.mock.assert_called_once_with("order-3")
        # no_ack means nothing was ever recorded, failure or not
        assert pel._entries == {}
