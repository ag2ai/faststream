import asyncio
from unittest.mock import MagicMock

import anyio
import pytest

from faststream import AckPolicy
from faststream.sqs import SQSMessage

from .basic import SQSTestcaseConfig


@pytest.mark.sqs()
@pytest.mark.connected()
@pytest.mark.asyncio()
class TestAckPolicy(SQSTestcaseConfig):
    async def test_ack_deletes_message(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker()
        done = asyncio.Event()

        @broker.subscriber(
            queue,
            ack_policy=AckPolicy.ACK,
            visibility_timeout=1,
            wait_time_seconds=1,
        )
        async def handler(msg: str) -> None:
            mock()
            done.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            with anyio.fail_after(self.timeout):
                await done.wait()
            # if ack had not deleted the message, visibility_timeout=1
            # would redeliver it within this window
            await asyncio.sleep(3.0)

        assert mock.call_count == 1, mock.call_count

    async def test_nack_on_error_redelivers(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker()
        done = asyncio.Event()

        @broker.subscriber(
            queue,
            ack_policy=AckPolicy.NACK_ON_ERROR,
            visibility_timeout=1,
            wait_time_seconds=1,
        )
        async def handler(msg: str) -> None:
            mock()
            if mock.call_count == 1:
                raise ValueError(msg)
            done.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            with anyio.fail_after(self.timeout):
                await done.wait()

        assert mock.call_count == 2, mock.call_count

    async def test_manual_ack(self, queue: str, mock: MagicMock) -> None:
        broker = self.get_broker(apply_types=True)
        done = asyncio.Event()

        @broker.subscriber(
            queue,
            ack_policy=AckPolicy.MANUAL,
            visibility_timeout=1,
            wait_time_seconds=1,
        )
        async def handler(body: str, msg: SQSMessage) -> None:
            mock()
            await msg.ack()
            done.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            with anyio.fail_after(self.timeout):
                await done.wait()
            await asyncio.sleep(3.0)

        assert mock.call_count == 1, mock.call_count
