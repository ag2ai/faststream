import asyncio
from unittest.mock import MagicMock, patch

import anyio
import pytest

from faststream import AckPolicy
from faststream.sqs.message import SQSMessage
from tests.brokers.base.consume import BrokerRealConsumeTestcase
from tests.tools import spy_decorator

from .basic import SQSTestcaseConfig


@pytest.mark.connected()
@pytest.mark.sqs()
class TestConsume(SQSTestcaseConfig, BrokerRealConsumeTestcase):
    @pytest.mark.asyncio()
    async def test_manual_ack_not_called_automatically(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        """With ``AckPolicy.MANUAL`` the framework must not delete the message."""
        event = asyncio.Event()

        consume_broker = self.get_broker(apply_types=True)

        args, kwargs = self.get_subscriber_params(queue, ack_policy=AckPolicy.MANUAL)

        @consume_broker.subscriber(*args, **kwargs)
        async def handler(body: str) -> None:
            mock(body)
            event.set()

        async with self.patch_broker(consume_broker) as br:
            await br.start()

            with patch.object(SQSMessage, "ack", spy_decorator(SQSMessage.ack)) as m:
                await asyncio.wait(
                    (
                        asyncio.create_task(br.publish("hello", queue)),
                        asyncio.create_task(event.wait()),
                    ),
                    timeout=self.timeout,
                )
                assert not m.mock.called

        assert event.is_set()
        mock.assert_called_once_with("hello")

    @pytest.mark.asyncio()
    async def test_concurrent_subscriber(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        event = asyncio.Event()
        event2 = asyncio.Event()

        broker = self.get_broker()

        args, kwargs = self.get_subscriber_params(queue, max_workers=2)

        @broker.subscriber(*args, **kwargs)
        async def handler(msg) -> None:
            mock()

            if event.is_set():
                event2.set()
            else:
                event.set()

            await asyncio.sleep(3.0)

        async with self.patch_broker(broker) as br:
            await br.start()

            for i in range(5):
                await br.publish(i, queue)

            await asyncio.wait(
                (
                    asyncio.create_task(event.wait()),
                    asyncio.create_task(event2.wait()),
                ),
                timeout=self.timeout,
            )

        assert event.is_set()
        assert event2.is_set()
        assert mock.call_count == 2, mock.call_count

    @pytest.mark.asyncio()
    async def test_extend_visibility_prevents_redelivery(
        self,
        queue: str,
        mock: MagicMock,
    ) -> None:
        broker = self.get_broker()
        done = asyncio.Event()

        args, kwargs = self.get_subscriber_params(
            queue,
            visibility_timeout=2,
            extend_visibility=True,
            wait_time_seconds=1,
        )

        @broker.subscriber(*args, **kwargs)
        async def handler(msg: str) -> None:
            mock()
            # run well past visibility_timeout: without the heartbeat SQS
            # would redeliver and mock.call_count would grow
            await asyncio.sleep(5.0)
            done.set()

        async with broker:
            await broker.start()
            await broker.publish("hello", queue)
            with anyio.fail_after(self.timeout):
                await done.wait()
            await asyncio.sleep(3.0)

        assert mock.call_count == 1, mock.call_count
