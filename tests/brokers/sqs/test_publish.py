import asyncio
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import Context
from faststream.sqs import SQSResponse
from tests.brokers.base.publish import BrokerPublishTestcase

from .basic import SQSMemoryTestcaseConfig, SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestPublish(SQSTestcaseConfig, BrokerPublishTestcase):
    @pytest.mark.asyncio()
    async def test_response(self, queue: str, mock: MagicMock) -> None:
        """A handler returning ``SQSResponse`` publishes it to the next queue."""
        event = asyncio.Event()

        pub_broker = self.get_broker(apply_types=True)

        @pub_broker.subscriber(queue)
        @pub_broker.publisher(queue + "1")
        async def handle():
            return SQSResponse(1, correlation_id="1")

        @pub_broker.subscriber(queue + "1")
        async def handle_next(msg=Context("message")) -> None:
            mock(body=msg.body, correlation_id=msg.correlation_id)
            event.set()

        async with self.patch_broker(pub_broker) as br:
            await br.start()

            await asyncio.wait(
                (
                    asyncio.create_task(br.publish("", queue, correlation_id="wrong")),
                    asyncio.create_task(event.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        mock.assert_called_once_with(body=b"1", correlation_id="1")

    @pytest.mark.asyncio()
    async def test_batch_publisher_real(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)
        publisher = broker.publisher(queue, batch=True)

        collected: list[int] = []
        done = asyncio.Event()

        @broker.subscriber(queue)
        async def handler(msg: int) -> None:
            collected.append(msg)
            if len(collected) == 3:
                done.set()

        async with broker:
            await broker.start()
            await publisher.publish(1, 2, 3)
            with anyio.fail_after(self.timeout):
                await done.wait()

        assert sorted(collected) == [1, 2, 3]


@pytest.mark.sqs()
@pytest.mark.asyncio()
class TestBatchPublisher(SQSMemoryTestcaseConfig):
    async def test_batch_publisher_publishes_each_message(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)
        publisher = broker.publisher(queue, batch=True)

        received: list[int] = []

        @broker.subscriber(queue)
        async def handler(msg: int) -> None:
            received.append(msg)

        async with self.patch_broker(broker) as br:
            await br.start()
            await publisher.publish(1, 2, 3)

        assert sorted(received) == [1, 2, 3]

    async def test_batch_publisher_decorator(self, queue: str) -> None:
        broker = self.get_broker(apply_types=True)

        received: list[int] = []

        @broker.subscriber(queue + "out")
        async def sink(msg: int) -> None:
            received.append(msg)

        @broker.publisher(queue + "out", batch=True)
        @broker.subscriber(queue)
        async def handler(msg: int) -> list[int]:
            return [msg, msg + 1]

        async with self.patch_broker(broker) as br:
            await br.start()
            await br.publish(1, queue)

        assert sorted(received) == [1, 2]
