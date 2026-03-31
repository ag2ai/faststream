import asyncio

import pytest

from faststream.mq.annotations import MQMessage
from faststream.middlewares import AckPolicy
from faststream.mq.message import MQRawMessage
from tests.marks import require_ibmmq

from .basic import MQMemoryTestcaseConfig, MQTestcaseConfig


@pytest.mark.mq()
@pytest.mark.asyncio()
class TestManualAckDirect(MQMemoryTestcaseConfig):
    async def test_get_one_requires_previous_message_to_be_settled(
        self, queue: str
    ) -> None:
        broker = self.get_broker()
        subscriber = broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)

        async with self.patch_broker(broker):
            await subscriber.start()
            try:
                await subscriber.put_test_message(MQRawMessage(body=b"one", queue=queue))
                await subscriber.put_test_message(MQRawMessage(body=b"two", queue=queue))

                first = await subscriber.get_one(timeout=self.timeout)
                assert first is not None

                with pytest.raises(RuntimeError, match="only one unsettled message"):
                    await subscriber.get_one(timeout=self.timeout)

                await first.ack()
                second = await subscriber.get_one(timeout=self.timeout)
                assert second is not None
                assert await second.decode() == b"two"
                await second.ack()
            finally:
                await subscriber.stop()

    async def test_iterator_requires_previous_message_to_be_settled(
        self, queue: str
    ) -> None:
        broker = self.get_broker()
        subscriber = broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)

        async with self.patch_broker(broker):
            await subscriber.start()
            try:
                await subscriber.put_test_message(MQRawMessage(body=b"one", queue=queue))
                await subscriber.put_test_message(MQRawMessage(body=b"two", queue=queue))

                iterator = subscriber.__aiter__()
                first = await asyncio.wait_for(iterator.__anext__(), timeout=self.timeout)

                with pytest.raises(RuntimeError, match="only one unsettled message"):
                    await asyncio.wait_for(iterator.__anext__(), timeout=self.timeout)

                await first.ack()
                second = await asyncio.wait_for(
                    subscriber.__aiter__().__anext__(),
                    timeout=self.timeout,
                )
                assert await second.decode() == b"two"
                await second.ack()
            finally:
                await subscriber.stop()


@require_ibmmq
@pytest.mark.connected()
@pytest.mark.mq()
@pytest.mark.asyncio()
class TestManualAckConnected(MQTestcaseConfig):
    async def test_handler_returning_without_settlement_blocks_next_delivery(
        self,
        queue: str,
    ) -> None:
        broker = self.get_broker(apply_types=True)
        first_received = asyncio.Event()
        second_received = asyncio.Event()
        held_messages = []
        seen: list[str] = []

        @broker.subscriber(queue, ack_policy=AckPolicy.MANUAL)
        async def handler(body: str, message: MQMessage) -> None:
            seen.append(body)
            held_messages.append(message)
            if len(seen) == 1:
                first_received.set()
            else:
                second_received.set()

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish("one", queue)
            await asyncio.wait_for(first_received.wait(), timeout=self.timeout)

            await br.publish("two", queue)
            await asyncio.sleep(0.5)
            assert not second_received.is_set()

            await held_messages[0].ack()
            await asyncio.wait_for(second_received.wait(), timeout=self.timeout)
            await held_messages[1].ack()

        assert seen == ["one", "two"]
