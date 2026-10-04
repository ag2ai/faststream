import asyncio

import pytest

from faststream import AckPolicy
from faststream.nats import JStream, PullSub

from .basic import NatsTestcaseConfig


@pytest.mark.connected()
@pytest.mark.nats()
@pytest.mark.asyncio()
class TestPullBatch(NatsTestcaseConfig):
    async def test_collects_full_and_partial_batches(
        self, queue: str, stream: JStream, event: asyncio.Event, event2: asyncio.Event
    ) -> None:
        """https://github.com/ag2ai/faststream/issues/2219."""
        broker = self.get_broker()
        received: list[list[str]] = []

        @broker.subscriber(
            queue,
            stream=stream,
            durable=queue,
            pull_sub=PullSub(batch_size=3, timeout=0.7, batch=True),
        )
        async def handler(bodies: list[str]) -> None:
            received.append(bodies)
            if len(received) == 1:
                event.set()
            else:
                event2.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            for i in range(3):
                await br.publish(str(i), queue)
                await asyncio.sleep(0.05)
            await asyncio.wait_for(event.wait(), 4)

            for i in range(3, 5):
                await br.publish(str(i), queue)
            await asyncio.wait_for(event2.wait(), 4)

        assert received == [["0", "1", "2"], ["3", "4"]]

    async def test_without_timeout_waits_for_full_batch(
        self, queue: str, stream: JStream, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()
        received: list[list[str]] = []

        @broker.subscriber(
            queue,
            stream=stream,
            durable=queue,
            pull_sub=PullSub(batch_size=2, timeout=None, batch=True),
        )
        async def handler(bodies: list[str]) -> None:
            received.append(bodies)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("0", queue)
            await asyncio.sleep(0.05)
            await br.publish("1", queue)
            await asyncio.wait_for(event.wait(), 4)

        assert received == [["0", "1"]]

    async def test_acknowledges_after_handler(
        self, queue: str, stream: JStream, event: asyncio.Event, event2: asyncio.Event
    ) -> None:
        broker = self.get_broker()
        received: list[list[str]] = []

        @broker.subscriber(
            queue,
            stream=stream,
            durable=queue,
            pull_sub=PullSub(batch_size=2, batch=True),
        )
        async def handler(bodies: list[str]) -> None:
            received.append(bodies)
            event.set()
            await event2.wait()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("0", queue)
            await br.publish("1", queue)
            await asyncio.wait_for(event.wait(), 4)

            js = (await br.connect()).jetstream()
            try:
                info = await js.consumer_info(queue, queue)
                assert info.num_ack_pending == 2
            finally:
                event2.set()

            async def all_acked() -> None:
                while True:
                    info = await js.consumer_info(queue, queue)
                    if info.num_ack_pending == info.num_pending == 0:
                        return
                    await asyncio.sleep(0.01)

            await asyncio.wait_for(all_acked(), 4)

        assert received == [["0", "1"]]

    async def test_nacks_failed_handler(
        self, queue: str, stream: JStream, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()
        attempts: list[list[str]] = []

        @broker.subscriber(
            queue,
            stream=stream,
            durable=queue,
            pull_sub=PullSub(batch_size=2, timeout=0.7, batch=True),
            ack_policy=AckPolicy.NACK_ON_ERROR,
        )
        async def handler(bodies: list[str]) -> None:
            attempts.append(bodies)
            if len(attempts) == 1:
                message = "retry this batch"
                raise RuntimeError(message)
            event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("0", queue)
            await br.publish("1", queue)
            await asyncio.wait_for(event.wait(), 5)

        assert attempts == [["0", "1"], ["0", "1"]]

    async def test_without_batch_handles_messages_individually(
        self, queue: str, stream: JStream, event: asyncio.Event
    ) -> None:
        broker = self.get_broker()
        received: list[str] = []

        @broker.subscriber(
            queue,
            stream=stream,
            durable=queue,
            pull_sub=PullSub(batch_size=3, timeout=0.7, batch=False),
        )
        async def handler(body: str) -> None:
            received.append(body)
            if len(received) == 2:
                event.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("0", queue)
            await br.publish("1", queue)
            await asyncio.wait_for(event.wait(), 4)

        assert received == ["0", "1"]
