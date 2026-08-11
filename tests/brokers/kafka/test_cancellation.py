import asyncio

import pytest

from faststream import AckPolicy

from .basic import KafkaTestcaseConfig


@pytest.mark.kafka()
@pytest.mark.connected()
@pytest.mark.asyncio()
@pytest.mark.filterwarnings(
    "ignore:AckPolicy.REJECT_ON_ERROR has the same effect as AckPolicy.ACK."
)
class TestKafkaCancellation(KafkaTestcaseConfig):
    async def test_graceful_shutdown_redelivers_interrupted_message(
        self,
        queue: str,
    ) -> None:
        group_id = f"{queue}-group"
        payload = f"{queue}-payload"
        started = asyncio.Event()
        redelivered = asyncio.Event()

        broker = self.get_broker(graceful_timeout=0.3)

        @broker.subscriber(
            queue,
            group_id=group_id,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
            auto_offset_reset="earliest",
        )
        async def handle(_: str) -> None:
            started.set()
            await asyncio.sleep(60)

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish(payload, queue)
            await asyncio.wait_for(started.wait(), timeout=self.timeout)
            await br.stop()

        restart_broker = self.get_broker()

        @restart_broker.subscriber(
            queue,
            group_id=group_id,
            ack_policy=AckPolicy.REJECT_ON_ERROR,
            auto_offset_reset="earliest",
        )
        async def handle_retry(msg: str) -> None:
            assert msg == payload
            redelivered.set()

        async with self.patch_broker(restart_broker) as br:
            await br.start()
            await asyncio.wait_for(redelivered.wait(), timeout=self.timeout)
